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
import software.amazon.awssdk.services.s3.model.NoSuchKeyException;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class S3OcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflStorageInitializer.class);

    private static final String OBJECT_MARKER_PREFIX = "0=ocfl_object";
    private static final String LAYOUT_SPEC = "ocfl_layout.json";

    private ObjectMapper objectMapper;
    private ObjectIdPathMapperBuilder objectIdPathMapperBuilder;

    public S3OcflStorageInitializer(ObjectMapper objectMapper, ObjectIdPathMapperBuilder objectIdPathMapperBuilder) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        this.objectIdPathMapperBuilder = Enforce.notNull(objectIdPathMapperBuilder, "objectIdPathMapperBuilder cannot be null");
    }

    // TODO do we need to support creating a repository within a bucket. ie allow for other repos in the same bucket or non-repo content?
    public ObjectIdPathMapper initializeStorage(S3ClientWrapper s3Client, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(s3Client, "s3Client cannot be null");
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        ensureBucketExists(s3Client);

        if (listRootObjects(s3Client).isEmpty()) {
            return initNewRepo(s3Client, ocflVersion, layoutConfig);
        } else {
            return validateExistingRepo(s3Client, ocflVersion, layoutConfig);
        }
    }

    private ObjectIdPathMapper validateExistingRepo(S3ClientWrapper s3Client, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        validateOcflVersion(s3Client, ocflVersion);

        var layoutSpec = readOcflLayout(s3Client);

        if (layoutSpec == null) {
            return validateLayoutByInspection(s3Client, layoutConfig);
        }

        return validateLayoutByConfig(s3Client, layoutSpec, layoutConfig);
    }

    private void validateOcflVersion(S3ClientWrapper s3Client, OcflVersion ocflVersion) {
        OcflVersion existingOcflVersion = null;

        for (var file : listRootObjects(s3Client)) {
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

    private ObjectIdPathMapper validateLayoutByConfig(S3ClientWrapper s3Client, LayoutSpec layoutSpec, LayoutConfig layoutConfig) {
        var expectedConfig = readLayoutConfig(s3Client, layoutSpec);

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new IllegalStateException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        return createObjectIdPathMapper(expectedConfig);
    }

    private ObjectIdPathMapper validateLayoutByInspection(S3ClientWrapper s3Client, LayoutConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new IllegalStateException(String.format(
                    "No storage layout configuration is defined in the OCFL repository in bucket %s. Layout must be configured programmatically.",
                    s3Client.bucket()));
        }

        var mapper = createObjectIdPathMapper(layoutConfig);
        var objectRoot = identifyRandomObjectRoot(s3Client, "");

        if (objectRoot != null) {
            var objectId = extractObjectId(s3Client, ObjectPaths.inventoryPath(objectRoot));
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

    private String identifyRandomObjectRoot(S3ClientWrapper s3Client, String prefix) {
        var response = s3Client.listDirectory(prefix);

        for (var object : response.contents()) {
            var fileName = object.key().substring(object.key().lastIndexOf('/') + 1);
            if (fileName.startsWith(OBJECT_MARKER_PREFIX)) {
                return (String) object.key().subSequence(0, object.key().lastIndexOf('/'));
            }
        }

        for (var commonPrefix : response.commonPrefixes()) {
            var root = identifyRandomObjectRoot(s3Client, commonPrefix.prefix());
            if (root != null) {
                return root;
            }
        }

        return null;
    }

    private String extractObjectId(S3ClientWrapper s3Client, String inventoryPath) {
        try (var stream = s3Client.downloadStream(inventoryPath)) {
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
            throw new CorruptObjectException(String.format("Missing inventory at %s in bucket %s", inventoryPath, s3Client.bucket()));
        }
    }

    private ObjectIdPathMapper initNewRepo(S3ClientWrapper s3Client, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository in the bucket {}", s3Client.bucket());

        var keys = new ArrayList<String>();

        try {
            keys.add(writeNamasteFile(s3Client, ocflVersion));
            keys.add(writeOcflSpec(s3Client, ocflVersion));
            keys.addAll(writeOcflLayout(s3Client, layoutConfig));
            return createObjectIdPathMapper(layoutConfig);
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository", e);
            s3Client.safeDeleteObjects(keys);
            throw e;
        }
    }

    private String writeOcflSpec(S3ClientWrapper s3Client, OcflVersion ocflVersion) {
        var ocflSpecFile = ocflVersion.getOcflVersion() + ".txt";
        try (var ocflSpecStream = this.getClass().getClassLoader().getResourceAsStream(ocflSpecFile)) {
            return uploadStream(s3Client, ocflSpecFile, ocflSpecStream);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String writeNamasteFile(S3ClientWrapper s3Client, OcflVersion ocflVersion) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflVersion());
        return s3Client.uploadBytes(namasteFile.fileName(), namasteFile.fileContent().getBytes(StandardCharsets.UTF_8));
    }

    private List<String> writeOcflLayout(S3ClientWrapper s3Client, LayoutConfig layoutConfig) {
        var keys = new ArrayList<String>();
        var spec = LayoutSpec.layoutSpecForConfig(layoutConfig);
        try {
            keys.add(s3Client.uploadBytes(LAYOUT_SPEC, objectMapper.writeValueAsBytes(spec)));
            // TODO versioning...
            keys.add(s3Client.uploadBytes(extensionLayoutSpecFile(spec), objectMapper.writeValueAsBytes(layoutConfig)));
            return keys;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ObjectIdPathMapper createObjectIdPathMapper(LayoutConfig layoutConfig) {
        return objectIdPathMapperBuilder.build(layoutConfig);
    }

    private LayoutSpec readOcflLayout(S3ClientWrapper s3Client) {
        try (var stream = s3Client.downloadStream(LAYOUT_SPEC)) {
            return read(stream, LayoutSpec.class);
        } catch (NoSuchKeyException e) {
            return null;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private LayoutConfig readLayoutConfig(S3ClientWrapper s3Client, LayoutSpec spec) {
        try (var stream = s3Client.downloadStream(extensionLayoutSpecFile(spec))) {
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

    private void ensureBucketExists(S3ClientWrapper s3Client) {
        try {
            s3Client.headBucket();
        } catch (RuntimeException e) {
            throw new IllegalStateException(String.format("Bucket %s does not exist or is not accessible.", s3Client.bucket()), e);
        }
    }

    private String uploadStream(S3ClientWrapper s3Client, String remotePath, InputStream stream) {
        try {
            return s3Client.uploadBytes(remotePath, stream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private List<String> listRootObjects(S3ClientWrapper s3Client) {
        return s3Client.listDirectory("").contents().stream()
                .map(S3Object::key)
                .collect(Collectors.toList());
    }

}
