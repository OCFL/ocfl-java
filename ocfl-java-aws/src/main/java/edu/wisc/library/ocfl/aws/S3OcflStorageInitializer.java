package edu.wisc.library.ocfl.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.LayoutSpec;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3OcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflStorageInitializer.class);

    private static final String OBJECT_MARKER_PREFIX = "0=ocfl_object";
    private static final String LAYOUT_SPEC = "ocfl_layout.json";

    private S3Client s3Client;
    private ObjectMapper objectMapper;
    private ObjectIdPathMapperBuilder objectIdPathMapperBuilder;

    public S3OcflStorageInitializer(S3Client s3Client, ObjectMapper objectMapper, ObjectIdPathMapperBuilder objectIdPathMapperBuilder) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        this.objectIdPathMapperBuilder = Enforce.notNull(objectIdPathMapperBuilder, "objectIdPathMapperBuilder cannot be null");
    }

    // TODO do we need to support creating a repository within a bucket. ie allow for other repos in the same bucket or non-repo content?
    public ObjectIdPathMapper initializeStorage(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notBlank(bucketName, "bucketName cannot be blank");
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        ensureBucketExists(bucketName);

        if (listRootObjects(bucketName).isEmpty()) {
            return initNewRepo(bucketName, ocflVersion, layoutConfig);
        } else {
            return validateExistingRepo(bucketName, ocflVersion, layoutConfig);
        }
    }

    private ObjectIdPathMapper validateExistingRepo(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        validateOcflVersion(bucketName, ocflVersion);

        var layoutSpec = readOcflLayout(bucketName);

        if (layoutSpec == null) {
            return validateLayoutByInspection(bucketName, layoutConfig);
        }

        return validateLayoutByConfig(bucketName, layoutSpec, layoutConfig);
    }

    private void validateOcflVersion(String bucketName, OcflVersion ocflVersion) {
        OcflVersion existingOcflVersion = null;

        for (var file : listRootObjects(bucketName)) {
            if (file.startsWith("0=")) {
                existingOcflVersion = OcflVersion.fromOcflVersionString(file.substring(2));
                break;
            }
        }

        if (existingOcflVersion == null) {
            throw new IllegalStateException("OCFL root is missing its root conformance declaration.");
        } else if (existingOcflVersion != ocflVersion) {
            throw new IllegalStateException(String.format("OCFL version mismatch. Expected: %s; Found: %s",
                    ocflVersion, existingOcflVersion));
        }
    }

    private ObjectIdPathMapper validateLayoutByConfig(String bucketName, LayoutSpec layoutSpec, LayoutConfig layoutConfig) {
        var expectedConfig = readLayoutConfig(bucketName, layoutSpec);

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new IllegalStateException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        return createObjectIdPathMapper(expectedConfig);
    }

    private ObjectIdPathMapper validateLayoutByInspection(String bucketName, LayoutConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new IllegalStateException(String.format(
                    "No storage layout configuration is defined in the OCFL repository in bucket %s. Layout must be configured programmatically.",
                    bucketName));
        }

        var mapper = createObjectIdPathMapper(layoutConfig);
        var objectRoot = identifyRandomObjectRoot(bucketName, "");

        if (objectRoot != null) {
            var objectId = extractObjectId(bucketName, ObjectPaths.inventoryPath(objectRoot));
            var expectedPath = mapper.map(objectId);

            if (!expectedPath.equals(objectRoot)) {
                throw new IllegalStateException(String.format(
                        "The OCFL client was configured to use the following layout: %s." +
                                " This layout does not match the layout of existing objects in the repository." +
                                " Found object %s stored at %s, but was expecting it to be stored at %s.",
                        layoutConfig, objectId, objectRoot, expectedPath
                ));
            }

            // TODO should the layout be written? even with this check it's not guaranteed to be correct
        }

        return mapper;
    }

    private String identifyRandomObjectRoot(String bucketName, String prefix) {
        var response = list(bucketName, prefix);

        for (var object : response.contents()) {
            var fileName = object.key().substring(object.key().lastIndexOf('/') + 1);
            if (fileName.startsWith(OBJECT_MARKER_PREFIX)) {
                return (String) object.key().subSequence(0, object.key().lastIndexOf('/'));
            }
        }

        for (var commonPrefix : response.commonPrefixes()) {
            var root = identifyRandomObjectRoot(bucketName, commonPrefix.prefix());
            if (root != null) {
                return root;
            }
        }

        return null;
    }

    private String extractObjectId(String bucketName, String inventoryPath) {
        try (var stream = downloadStream(bucketName, inventoryPath)) {
            var map = read(stream, Map.class);
            var id = map.get("id");

            if (id == null) {
                throw new InvalidInventoryException(String.format("Inventory file at %s does not contain an id.", inventoryPath));
            }

            return (String) id;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        } catch (NoSuchKeyException e) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException(String.format("Missing inventory at %s in bucket %s", inventoryPath, bucketName));
        }
    }

    private ObjectIdPathMapper initNewRepo(String bucketName, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository in the bucket {}", bucketName);

        var keys = new ArrayList<String>();

        try {
            keys.add(writeNamasteFile(bucketName, ocflVersion));
            keys.add(writeOcflSpec(bucketName, ocflVersion));
            keys.addAll(writeOcflLayout(bucketName, layoutConfig));
            return createObjectIdPathMapper(layoutConfig);
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository", e);
            safeDeleteObjects(bucketName, keys);
            throw e;
        }
    }

    private String writeOcflSpec(String bucketName, OcflVersion ocflVersion) {
        var ocflSpecFile = ocflVersion.getOcflVersion() + ".txt";
        try (var ocflSpecStream = this.getClass().getClassLoader().getResourceAsStream(ocflSpecFile)) {
            return uploadStream(ocflSpecStream, bucketName, ocflSpecFile);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String writeNamasteFile(String bucketName, OcflVersion ocflVersion) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflVersion());
        return uploadBytes(namasteFile.fileContent().getBytes(), bucketName, namasteFile.fileName());
    }

    private List<String> writeOcflLayout(String bucketName, LayoutConfig layoutConfig) {
        var keys = new ArrayList<String>();
        var spec = LayoutSpec.layoutSpecForConfig(layoutConfig);
        try {
            keys.add(uploadBytes(objectMapper.writeValueAsBytes(spec), bucketName,
                    LAYOUT_SPEC));
            // TODO versioning...
            keys.add(uploadBytes(objectMapper.writeValueAsBytes(layoutConfig), bucketName,
                    extensionLayoutSpecFile(spec)));
            return keys;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ObjectIdPathMapper createObjectIdPathMapper(LayoutConfig layoutConfig) {
        return objectIdPathMapperBuilder.build(layoutConfig);
    }

    private LayoutSpec readOcflLayout(String bucketName) {
        try (var stream = downloadStream(bucketName, LAYOUT_SPEC)) {
            return read(stream, LayoutSpec.class);
        } catch (NoSuchKeyException | IOException e) {
            return null;
        }
    }

    private LayoutConfig readLayoutConfig(String bucketName, LayoutSpec spec) {
        try (var stream = downloadStream(bucketName, extensionLayoutSpecFile(spec))) {
            return read(stream, spec.getKey().getConfigClass());
        } catch (NoSuchKeyException e) {
            throw new IllegalStateException(String.format("Missing layout extension configuration at %s", extensionLayoutSpecFile(spec)));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String extensionLayoutSpecFile(LayoutSpec spec) {
        return String.format("extension-%s.json", spec.getKey());
    }

    private <T> T read(InputStream stream, Class<T> clazz) {
        try {
            return objectMapper.readValue(stream, clazz);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void ensureBucketExists(String bucketName) {
        try {
            s3Client.headBucket(HeadBucketRequest.builder()
                    .bucket(bucketName)
                    .build());
        } catch (RuntimeException e) {
            throw new IllegalStateException(String.format("Bucket %s does not exist or is not accessible.", bucketName), e);
        }
    }

    private String uploadStream(InputStream stream, String bucketName, String remotePath) {
        try {
            return uploadBytes(stream.readAllBytes(), bucketName, remotePath);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String uploadBytes(byte[] bytes, String bucketName, String remotePath) {
        LOG.debug("Uploading bytes to bucket {} key {}", bucketName, remotePath);
        s3Client.putObject(PutObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath)
                .build(), RequestBody.fromBytes(bytes));
        return remotePath;
    }

    private InputStream downloadStream(String bucketName, String remotePath) {
        LOG.debug("Downloading bucket {} key {} to stream", bucketName, remotePath);
        return s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucketName)
                .key(remotePath)
                .build());
    }

    private List<String> listRootObjects(String bucketName) {
        return list(bucketName, "").contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

    private ListObjectsV2Response list(String bucketName, String path) {
        var prefix = path;

        if (!prefix.isEmpty() && !prefix.endsWith("/")) {
            prefix = prefix + "/";
        }

        return s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucketName)
                .delimiter("/")
                .prefix(prefix)
                .build());
    }

    private void deleteObjects(String bucketName, Collection<String> objectKeys) {
        LOG.debug("Deleting objects in bucket {}: {}", bucketName, objectKeys);

        if (!objectKeys.isEmpty()) {
            var objectIds = objectKeys.stream()
                    .map(key -> ObjectIdentifier.builder().key(key).build())
                    .collect(Collectors.toList());

            s3Client.deleteObjects(DeleteObjectsRequest.builder()
                    .bucket(bucketName)
                    .delete(Delete.builder()
                            .objects(objectIds)
                            .build())
                    .build());
        }
    }

    private void safeDeleteObjects(String bucketName, Collection<String> objectKeys) {
        try {
            deleteObjects(bucketName, objectKeys);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup objects in bucket {}: {}", bucketName, objectKeys, e);
        }
    }

}
