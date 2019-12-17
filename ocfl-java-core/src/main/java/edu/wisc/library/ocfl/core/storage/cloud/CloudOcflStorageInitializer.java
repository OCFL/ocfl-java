package edu.wisc.library.ocfl.core.storage.cloud;

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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Initializes an OCFL repository in cloud storage. If the repository does not already exist, a new one is created. If it
 * does exist, the client configuration is verified and {@link ObjectIdPathMapper} is created.
 */
public class CloudOcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(CloudOcflStorageInitializer.class);

    private static final String MIME_TEXT = "text/plain; charset=UTF-8";
    private static final String MIME_JSON = "application/json";
    private static final String OBJECT_MARKER_PREFIX = "0=ocfl_object";
    private static final String LAYOUT_SPEC = "ocfl_layout.json";

    private CloudClient cloudClient;
    private ObjectMapper objectMapper;
    private ObjectIdPathMapperBuilder objectIdPathMapperBuilder;

    public CloudOcflStorageInitializer(CloudClient cloudClient, ObjectMapper objectMapper, ObjectIdPathMapperBuilder objectIdPathMapperBuilder) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        this.objectIdPathMapperBuilder = Enforce.notNull(objectIdPathMapperBuilder, "objectIdPathMapperBuilder cannot be null");
    }

    // TODO do we need to support creating a repository within a bucket. ie allow for other repos in the same bucket or non-repo content?
    public ObjectIdPathMapper initializeStorage(OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        ensureBucketExists();

        if (listRootObjects().isEmpty()) {
            return initNewRepo(ocflVersion, layoutConfig);
        } else {
            return validateExistingRepo(ocflVersion, layoutConfig);
        }
    }

    private ObjectIdPathMapper validateExistingRepo(OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        validateOcflVersion(ocflVersion);

        var layoutSpec = readOcflLayout();

        if (layoutSpec == null) {
            return validateLayoutByInspection(layoutConfig);
        }

        return validateLayoutByConfig(layoutSpec, layoutConfig);
    }

    private void validateOcflVersion(OcflVersion ocflVersion) {
        OcflVersion existingOcflVersion = null;

        for (var file : listRootObjects()) {
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

    private ObjectIdPathMapper validateLayoutByConfig(LayoutSpec layoutSpec, LayoutConfig layoutConfig) {
        var expectedConfig = readLayoutConfig(layoutSpec);

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new IllegalStateException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        return createObjectIdPathMapper(expectedConfig);
    }

    private ObjectIdPathMapper validateLayoutByInspection(LayoutConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new IllegalStateException(String.format(
                    "No storage layout configuration is defined in the OCFL repository in bucket %s. Layout must be configured programmatically.",
                    cloudClient.bucket()));
        }

        var mapper = createObjectIdPathMapper(layoutConfig);
        var objectRoot = identifyRandomObjectRoot("");

        if (objectRoot != null) {
            var objectId = extractObjectId(ObjectPaths.inventoryPath(objectRoot));
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

    private String identifyRandomObjectRoot(String prefix) {
        var response = cloudClient.listDirectory(prefix);

        for (var object : response.getObjects()) {
            if (object.getFileName().startsWith(OBJECT_MARKER_PREFIX)) {
                return (String) object.getKey().subSequence(0, object.getKey().lastIndexOf('/'));
            }
        }

        for (var dir : response.getDirectories()) {
            var root = identifyRandomObjectRoot(dir.getPath());
            if (root != null) {
                return root;
            }
        }

        return null;
    }

    private String extractObjectId(String inventoryPath) {
        try (var stream = cloudClient.downloadStream(inventoryPath)) {
            var map = read(stream, Map.class);
            var id = map.get("id");

            if (id == null) {
                throw new InvalidInventoryException(String.format("Inventory file at %s does not contain an id.", inventoryPath));
            }

            return (String) id;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        } catch (KeyNotFoundException e) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException(String.format("Missing inventory at %s in bucket %s", inventoryPath, cloudClient.bucket()));
        }
    }

    private ObjectIdPathMapper initNewRepo(OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository in the bucket {}", cloudClient.bucket());

        var keys = new ArrayList<String>();

        try {
            keys.add(writeNamasteFile(ocflVersion));
            keys.add(writeOcflSpec(ocflVersion));
            keys.addAll(writeOcflLayout(layoutConfig));
            return createObjectIdPathMapper(layoutConfig);
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository", e);
            cloudClient.safeDeleteObjects(keys);
            throw e;
        }
    }

    private String writeOcflSpec(OcflVersion ocflVersion) {
        var ocflSpecFile = ocflVersion.getOcflVersion() + ".txt";
        try (var ocflSpecStream = this.getClass().getClassLoader().getResourceAsStream(ocflSpecFile)) {
            return uploadStream(ocflSpecFile, ocflSpecStream);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String writeNamasteFile(OcflVersion ocflVersion) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflVersion());
        return cloudClient.uploadBytes(namasteFile.fileName(), namasteFile.fileContent().getBytes(StandardCharsets.UTF_8),
                MIME_TEXT);
    }

    private List<String> writeOcflLayout(LayoutConfig layoutConfig) {
        var keys = new ArrayList<String>();
        var spec = LayoutSpec.layoutSpecForConfig(layoutConfig);
        try {
            keys.add(cloudClient.uploadBytes(LAYOUT_SPEC, objectMapper.writeValueAsBytes(spec), MIME_JSON));
            // TODO versioning...
            keys.add(cloudClient.uploadBytes(extensionLayoutSpecFile(spec),
                    objectMapper.writeValueAsBytes(layoutConfig), MIME_JSON));
            return keys;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ObjectIdPathMapper createObjectIdPathMapper(LayoutConfig layoutConfig) {
        return objectIdPathMapperBuilder.build(layoutConfig);
    }

    private LayoutSpec readOcflLayout() {
        try (var stream = cloudClient.downloadStream(LAYOUT_SPEC)) {
            return read(stream, LayoutSpec.class);
        } catch (KeyNotFoundException e) {
            return null;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private LayoutConfig readLayoutConfig(LayoutSpec spec) {
        try (var stream = cloudClient.downloadStream(extensionLayoutSpecFile(spec))) {
            return read(stream, spec.getKey().getConfigClass());
        } catch (KeyNotFoundException e) {
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

    private void ensureBucketExists() {
        if (!cloudClient.bucketExists()) {
            throw new IllegalStateException(String.format("Bucket %s does not exist or is not accessible.", cloudClient.bucket()));
        }
    }

    private String uploadStream(String remotePath, InputStream stream) {
        try {
            return cloudClient.uploadBytes(remotePath, stream.readAllBytes(), MIME_TEXT);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private List<String> listRootObjects() {
        return cloudClient.listDirectory("").getObjects().stream()
                .map(ListResult.ObjectListing::getKey)
                .collect(Collectors.toList());
    }

}
