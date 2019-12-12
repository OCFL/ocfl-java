package edu.wisc.library.ocfl.core.storage;

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
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prepares an OCFL storage root for use.
 */
public class FileSystemOcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorageInitializer.class);

    private ObjectMapper objectMapper;
    private ObjectIdPathMapperBuilder objectIdPathMapperBuilder;

    public FileSystemOcflStorageInitializer(ObjectMapper objectMapper, ObjectIdPathMapperBuilder objectIdPathMapperBuilder) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        this.objectIdPathMapperBuilder = Enforce.notNull(objectIdPathMapperBuilder, "objectIdPathMapperBuilder cannot be null");
    }

    /**
     * Prepares an OCFL storage root for use. If the root does not exist, it is created. If the root does exist, then it
     * ensures that the storage layout is configured correctly.
     *
     * @param repositoryRoot the OCFL storage root
     * @param ocflVersion the OCFL version, must match the version declared in the storage root
     * @param layoutConfig the storage layout configuration, if null the configuration will be loaded from disk
     * @return ObjectIdPathMapper configured for the repository's storage layout
     */
    public ObjectIdPathMapper initializeStorage(Path repositoryRoot, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        if (!Files.exists(repositoryRoot)) {
            SafeFiles.createDirectories(repositoryRoot);
        } else {
            Enforce.expressionTrue(Files.isDirectory(repositoryRoot), repositoryRoot,
                    "repositoryRoot must be a directory");
        }

        if (!FileUtil.hasChildren(repositoryRoot)) {
            return initNewRepo(repositoryRoot, ocflVersion, layoutConfig);
        } else {
            return validateExistingRepo(repositoryRoot, ocflVersion, layoutConfig);
        }
    }

    private ObjectIdPathMapper validateExistingRepo(Path repositoryRoot, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        validateOcflVersion(repositoryRoot, ocflVersion);

        var layoutSpec = readOcflLayout(repositoryRoot);

        if (layoutSpec == null) {
            return validateLayoutByInspection(repositoryRoot, layoutConfig);
        }

        return validateLayoutByConfig(repositoryRoot, layoutSpec, layoutConfig);
    }

    private void validateOcflVersion(Path repositoryRoot, OcflVersion ocflVersion) {
        OcflVersion existingOcflVersion = null;

        for (var file : repositoryRoot.toFile().listFiles()) {
            if (file.isFile() && file.getName().startsWith("0=")) {
                existingOcflVersion = OcflVersion.fromOcflVersionString(file.getName().substring(2));
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

    private ObjectIdPathMapper validateLayoutByConfig(Path repositoryRoot, LayoutSpec layoutSpec, LayoutConfig layoutConfig) {
        var expectedConfig = readLayoutConfig(repositoryRoot, layoutSpec);

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new IllegalStateException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        return createObjectIdPathMapper(expectedConfig);
    }

    private ObjectIdPathMapper validateLayoutByInspection(Path repositoryRoot, LayoutConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new IllegalStateException(String.format(
                    "No storage layout configuration is defined in the OCFL repository at %s. Layout must be configured programmatically.",
                    repositoryRoot));
        }

        var mapper = createObjectIdPathMapper(layoutConfig);
        var objectRoot = identifyRandomObjectRoot(repositoryRoot);

        if (objectRoot != null) {
            var objectId = extractObjectId(ObjectPaths.inventoryPath(objectRoot));
            var expectedPath = Paths.get(mapper.map(objectId));
            var actualPath = repositoryRoot.relativize(objectRoot);

            if (!expectedPath.equals(actualPath)) {
                throw new IllegalStateException(String.format(
                        "The OCFL client was configured to use the following layout: %s." +
                                " This layout does not match the layout of existing objects in the repository." +
                                " Found object %s stored at %s, but was expecting it to be stored at %s.",
                        layoutConfig, objectId, actualPath, expectedPath
                ));
            }

            // TODO should the layout be written? even with this check it's not guaranteed to be correct
        }

        return mapper;
    }

    private Path identifyRandomObjectRoot(Path root) {
        var ref = new AtomicReference<Path>();
        var objectMarkerPrefix = "0=ocfl_object";

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().startsWith(objectMarkerPrefix)) {
                        ref.set(file.getParent());
                        return FileVisitResult.TERMINATE;
                    }
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        return ref.get();
    }

    private String extractObjectId(Path inventoryPath) {
        if (Files.notExists(inventoryPath)) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException("Missing inventory at " + inventoryPath);
        }

        var map = read(inventoryPath, Map.class);
        var id = map.get("id");

        if (id == null) {
            throw new InvalidInventoryException(String.format("Inventory file at %s does not contain an id.", inventoryPath));
        }

        return (String) id;
    }

    private ObjectIdPathMapper initNewRepo(Path repositoryRoot, OcflVersion ocflVersion, LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository at {}", repositoryRoot);

        try {
            new NamasteTypeFile(ocflVersion.getOcflVersion()).writeFile(repositoryRoot);
            writeOcflSpec(repositoryRoot, ocflVersion);
            writeOcflLayout(repositoryRoot, layoutConfig);
            return createObjectIdPathMapper(layoutConfig);
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository at {}", repositoryRoot, e);
            FileUtil.safeDeletePath(repositoryRoot);
            throw e;
        }
    }

    private void writeOcflSpec(Path repositoryRoot, OcflVersion ocflVersion) {
        var ocflSpecFile = ocflVersion.getOcflVersion() + ".txt";
        try (var ocflSpecStream = this.getClass().getClassLoader().getResourceAsStream(ocflSpecFile)) {
            Files.copy(ocflSpecStream, repositoryRoot.resolve(ocflSpecFile));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void writeOcflLayout(Path repositoryRoot, LayoutConfig layoutConfig) {
        var spec = LayoutSpec.layoutSpecForConfig(layoutConfig);
        try {
            objectMapper.writeValue(ocflLayoutPath(repositoryRoot).toFile(), spec);
            // TODO versioning...
            objectMapper.writeValue(layoutExtensionConfigPath(repositoryRoot, spec).toFile(), layoutConfig);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private ObjectIdPathMapper createObjectIdPathMapper(LayoutConfig layoutConfig) {
        return objectIdPathMapperBuilder.build(layoutConfig);
    }

    private LayoutSpec readOcflLayout(Path repositoryRoot) {
        var layoutPath = ocflLayoutPath(repositoryRoot);

        if (Files.exists(layoutPath)) {
            return read(layoutPath, LayoutSpec.class);
        }

        return null;
    }

    private LayoutConfig readLayoutConfig(Path repositoryRoot, LayoutSpec spec) {
        var configPath = layoutExtensionConfigPath(repositoryRoot, spec);

        if (Files.exists(configPath)) {
            return read(configPath, spec.getKey().getConfigClass());
        } else {
            throw new IllegalStateException(String.format("Missing layout extension configuration at %s", configPath));
        }
    }

    private <T> T read(Path path, Class<T> clazz) {
        try {
            return objectMapper.readValue(path.toFile(), clazz);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private Path ocflLayoutPath(Path repositoryRoot) {
        return repositoryRoot.resolve("ocfl_layout.json");
    }

    private Path layoutExtensionConfigPath(Path repositoryRoot, LayoutSpec spec) {
        return repositoryRoot.resolve(String.format("extension-%s.json", spec.getKey()));
    }

}
