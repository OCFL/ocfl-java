/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.storage.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.OcflExtensionRegistry;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflLayout;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Prepares an OCFL storage root for use.
 */
public class FileSystemOcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorageInitializer.class);

    private static final String OBJECT_MARKER_PREFIX = "0=ocfl_object";

    private final ObjectMapper objectMapper;

    public FileSystemOcflStorageInitializer(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
    }

    /**
     * Prepares an OCFL storage root for use. If the root does not exist, it is created. If the root does exist, then it
     * ensures that the storage layout is configured correctly.
     *
     * @param repositoryRoot the OCFL storage root
     * @param ocflVersion the OCFL version, must match the version declared in the storage root
     * @param layoutConfig the storage layout configuration, if null the configuration will be loaded from disk
     * @return OCFL storage layout extension configured for the repo
     */
    public OcflStorageLayoutExtension initializeStorage(Path repositoryRoot, OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        if (!Files.exists(repositoryRoot)) {
            UncheckedFiles.createDirectories(repositoryRoot);
        } else {
            Enforce.expressionTrue(Files.isDirectory(repositoryRoot), repositoryRoot,
                    "repositoryRoot must be a directory");
        }

        OcflStorageLayoutExtension layoutExtension;

        if (!FileUtil.hasChildren(repositoryRoot)) {
            layoutExtension = initNewRepo(repositoryRoot, ocflVersion, layoutConfig);
        } else {
            layoutExtension = validateExistingRepo(repositoryRoot, ocflVersion, layoutConfig);
        }

        LOG.info("OCFL repository is configured to use OCFL storage layout extension {} implemented by {}",
                layoutExtension.getExtensionName(), layoutExtension.getClass());

        return layoutExtension;
    }

    private OcflStorageLayoutExtension validateExistingRepo(Path repositoryRoot, OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        validateOcflVersion(repositoryRoot, ocflVersion);

        var ocflLayout = readOcflLayout(repositoryRoot);

        if (ocflLayout == null) {
            LOG.debug("OCFL layout extension not specified");
            return validateLayoutByInspection(repositoryRoot, layoutConfig);
        }

        LOG.debug("OCFL layout extension: {}", ocflLayout.getExtension());

        return validateLayoutByConfig(repositoryRoot, ocflLayout, layoutConfig);
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

    private OcflStorageLayoutExtension validateLayoutByConfig(Path repositoryRoot, OcflLayout ocflLayout, OcflExtensionConfig layoutConfig) {
        var layoutExtension = loadLayoutExtension(ocflLayout.getExtension());
        var expectedConfig = readLayoutConfig(repositoryRoot, ocflLayout, layoutExtension.getExtensionConfigClass());

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new IllegalStateException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        layoutExtension.init(expectedConfig);
        return layoutExtension;
    }

    private OcflStorageLayoutExtension validateLayoutByInspection(Path repositoryRoot, OcflExtensionConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new IllegalStateException(String.format(
                    "No storage layout configuration is defined in the OCFL repository at %s. Layout must be configured programmatically.",
                    repositoryRoot));
        }

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);
        var objectRoot = identifyRandomObjectRoot(repositoryRoot);

        if (objectRoot != null) {
            var objectId = extractObjectId(ObjectPaths.inventoryPath(objectRoot));
            var expectedPath = Paths.get(layoutExtension.mapObjectId(objectId));
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

        return layoutExtension;
    }

    private Path identifyRandomObjectRoot(Path root) {
        var ref = new AtomicReference<Path>();

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().startsWith(OBJECT_MARKER_PREFIX)) {
                        ref.set(file.getParent());
                        return FileVisitResult.TERMINATE;
                    }
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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

    private OcflStorageLayoutExtension initNewRepo(Path repositoryRoot, OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository at {}", repositoryRoot);

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);

        try {
            new NamasteTypeFile(ocflVersion.getOcflVersion()).writeFile(repositoryRoot);
            writeOcflSpec(repositoryRoot, ocflVersion);
            writeOcflLayout(repositoryRoot, layoutConfig, layoutExtension.getDescription());
            return layoutExtension;
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
            throw new UncheckedIOException(e);
        }
    }

    private void writeOcflLayout(Path repositoryRoot, OcflExtensionConfig layoutConfig, String description) {
        var spec = new OcflLayout()
                .setExtension(layoutConfig.getExtensionName())
                .setDescription(description);
        try {
            objectMapper.writeValue(ocflLayoutPath(repositoryRoot).toFile(), spec);
            objectMapper.writeValue(layoutExtensionConfigPath(repositoryRoot, layoutConfig.getExtensionName()).toFile(), layoutConfig);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private OcflStorageLayoutExtension loadAndInitLayoutExtension(OcflExtensionConfig layoutConfig) {
        var layoutExtension = loadLayoutExtension(layoutConfig.getExtensionName());
        layoutExtension.init(layoutConfig);
        return layoutExtension;
    }

    private OcflStorageLayoutExtension loadLayoutExtension(String extensionName) {
        return OcflExtensionRegistry.<OcflStorageLayoutExtension>lookup(extensionName)
                .orElseThrow(() -> new IllegalStateException(
                        String.format("Failed to find an implementation for storage layout extension %s", extensionName)));
    }

    private OcflLayout readOcflLayout(Path repositoryRoot) {
        var layoutPath = ocflLayoutPath(repositoryRoot);

        if (Files.exists(layoutPath)) {
            return read(layoutPath, OcflLayout.class);
        }

        return null;
    }

    private OcflExtensionConfig readLayoutConfig(Path repositoryRoot, OcflLayout ocflLayout, Class<? extends OcflExtensionConfig> clazz) {
        var configPath = layoutExtensionConfigPath(repositoryRoot, ocflLayout.getExtension());

        if (Files.exists(configPath)) {
            return read(configPath, clazz);
        } else {
            // No config found, create default config object
            return initClass(clazz);
        }
    }

    private <T> T initClass(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException(String.format("Failed to init OCFL storage layout extension configuration class %s", clazz), e);
        }
    }

    private <T> T read(Path path, Class<T> clazz) {
        try {
            return objectMapper.readValue(path.toFile(), clazz);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path ocflLayoutPath(Path repositoryRoot) {
        return repositoryRoot.resolve(OcflConstants.OCFL_LAYOUT);
    }

    private Path layoutExtensionConfigPath(Path repositoryRoot, String extensionName) {
        return repositoryRoot.resolve(extensionName + ".json");
    }

}
