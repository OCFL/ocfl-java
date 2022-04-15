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

package edu.wisc.library.ocfl.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.exception.OcflJavaException;
import edu.wisc.library.ocfl.api.exception.OcflNoSuchFileException;
import edu.wisc.library.ocfl.api.exception.RepositoryConfigurationException;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.OcflExtensionRegistry;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflLayout;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
import edu.wisc.library.ocfl.core.storage.common.Listing;
import edu.wisc.library.ocfl.core.storage.common.Storage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Initializes an OCFL repository. If the repository does not already exist, a new one is created. If it
 * does exist, the client configuration is verified and a {@link OcflStorageLayoutExtension} is created.
 */
public class DefaultOcflStorageInitializer implements OcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflStorageInitializer.class);

    private static final String SPECS_DIR = "ocfl-specs/";
    private static final String EXT_SPEC = "ocfl_extensions_1.0.md";
    private static final String MEDIA_TYPE_TEXT = "text/plain; charset=UTF-8";
    private static final String MEDIA_TYPE_JSON = "application/json; charset=UTF-8";

    private final Storage storage;
    private final ObjectMapper objectMapper;

    public DefaultOcflStorageInitializer(Storage storage,
                                         ObjectMapper objectMapper) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public RepositoryConfig initializeStorage(OcflVersion ocflVersion,
                                              OcflExtensionConfig layoutConfig,
                                              ExtensionSupportEvaluator supportEvaluator) {
        RepositoryConfig config;

        if (directoryIsEmpty("")) {
            config = initNewRepo(ocflVersion, layoutConfig);
        } else {
            config = loadAndValidateExistingRepo(ocflVersion, layoutConfig);
            // This is only validating currently and does not load anything
            loadRepositoryExtensions(supportEvaluator);
        }

        LOG.info("OCFL repository is configured to adhere to OCFL {} and use OCFL storage layout extension {}",
                config.getOcflVersion().getRawVersion(),
                config.getStorageLayoutExtension().getExtensionName());

        return config;
    }

    private RepositoryConfig loadAndValidateExistingRepo(OcflVersion ocflVersion,
                                                         OcflExtensionConfig layoutConfig) {
        var existingVersion = identifyExistingVersion();
        var resolvedVersion = existingVersion;

        if (ocflVersion != null && existingVersion.compareTo(ocflVersion) < 0) {
            upgradeOcflRepo(existingVersion, ocflVersion);
            resolvedVersion = ocflVersion;
        }

        var ocflLayout = readOcflLayout();
        OcflStorageLayoutExtension extension;

        if (ocflLayout == null) {
            LOG.debug("OCFL layout extension not specified");
            extension = validateLayoutByInspection(layoutConfig);
        } else {
            LOG.debug("Found specified OCFL layout extension: {}", ocflLayout.getExtension());
            extension = loadLayoutByConfig(ocflLayout);
        }

        return new RepositoryConfig(resolvedVersion, extension);
    }

    private OcflVersion identifyExistingVersion() {
        OcflVersion foundVersion = null;

        for (var version : OcflVersion.values()) {
            var fileName = new NamasteTypeFile(version.getOcflVersion()).fileName();
            try {
                var contents = storage.readToString(fileName);
                foundVersion = OcflVersion.fromOcflVersionString(contents);
                break;
            } catch (OcflNoSuchFileException e) {
                LOG.debug("OCFL root namaste file {} does not exist", fileName);
            }
        }

        if (foundVersion == null) {
            throw new RepositoryConfigurationException("OCFL root is missing a namaste file, eg. 0=ocfl_1.0.");
        }

        return foundVersion;
    }

    private void upgradeOcflRepo(OcflVersion currentVersion, OcflVersion newVersion) {
        LOG.info("This is an OCFL {} repository, but was programmatically configured to create OCFL {} objects. " +
                        "Upgrading the OCFL repository to {}. Note, existing objects will NOT be upgraded.",
                currentVersion.getRawVersion(), newVersion.getRawVersion(), newVersion.getRawVersion());

        try {
            writeNamasteFile(newVersion);
            writeOcflSpec(newVersion);
            storage.deleteFile(new NamasteTypeFile(currentVersion.getOcflVersion()).fileName());
        } catch (RuntimeException e) {
            throw new OcflJavaException(String.format("Failed to upgrade OCFL repository to version %s",
                    newVersion.getRawVersion()), e);
        }
    }

    private OcflStorageLayoutExtension loadLayoutByConfig(OcflLayout ocflLayout) {
        var layoutExtension = loadLayoutExtension(ocflLayout.getExtension());
        var expectedConfig = readLayoutConfig(ocflLayout, layoutExtension.getExtensionConfigClass());
        layoutExtension.init(expectedConfig);
        return layoutExtension;
    }

    private OcflStorageLayoutExtension validateLayoutByInspection(OcflExtensionConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new RepositoryConfigurationException(
                    "No storage layout configuration is defined in the OCFL repository. Layout must be configured programmatically.");
        }

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);
        var objectRoot = identifyRandomObjectRoot();

        if (objectRoot != null) {
            var objectId = extractObjectId(ObjectPaths.inventoryPath(objectRoot));
            var expectedPath = layoutExtension.mapObjectId(objectId);

            if (!expectedPath.equals(objectRoot)) {
                throw new RepositoryConfigurationException(String.format(
                        "The OCFL client was configured to use the following layout: %s." +
                                " This layout does not match the layout of existing objects in the repository." +
                                " Found object %s stored at %s, but was expecting it to be stored at %s.",
                        layoutConfig, objectId, objectRoot, expectedPath
                ));
            }
        }

        return layoutExtension;
    }

    private String identifyRandomObjectRoot() {
        try (var iter = storage.iterateObjects()) {
            if (iter.hasNext()) {
                return iter.next();
            }
            return null;
        }
    }

    private String extractObjectId(String inventoryPath) {
        try (var stream = storage.read(inventoryPath)) {
            var map = read(stream, Map.class);
            var id = map.get("id");

            if (id == null) {
                throw new InvalidInventoryException(String.format("Inventory file at %s does not contain an id.", inventoryPath));
            }

            return (String) id;
        } catch (IOException e) {
            throw OcflIOException.from(e);
        } catch (OcflNoSuchFileException e) {
            throw new CorruptObjectException(String.format("Missing inventory at %s", inventoryPath));
        }
    }

    private RepositoryConfig initNewRepo(OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        if (ocflVersion == null) {
            ocflVersion = OcflConstants.DEFAULT_OCFL_VERSION;
        }

        LOG.info("Initializing new OCFL repository");

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);

        try {
            storage.createDirectories("");
            writeNamasteFile(ocflVersion);
            writeOcflSpec(ocflVersion);
            writeOcflLayout(layoutConfig, layoutExtension.getDescription());
            writeOcflLayoutSpec(layoutConfig);
            writeSpecFile(this.getClass().getClassLoader(), EXT_SPEC);
            return new RepositoryConfig(ocflVersion, layoutExtension);
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository", e);
            try {
                storage.deleteDirectory("");
            } catch (RuntimeException e1) {
                LOG.error("Failed to cleanup OCFL repository root", e1);
            }
            throw e;
        }
    }

    private void loadRepositoryExtensions(ExtensionSupportEvaluator supportEvaluator) {
        // Currently, this just ensures that the repository does not use any extensions that ocfl-java does not support
        list(OcflConstants.EXTENSIONS_DIR).stream()
                .filter(Listing::isDirectory).forEach(dir -> {
                    supportEvaluator.checkSupport(dir.getRelativePath());
                });
    }

    private void writeOcflSpec(OcflVersion ocflVersion) {
        writeSpecFile(this.getClass().getClassLoader(), ocflVersion.getOcflVersion() + ".txt");
    }

    private void writeOcflLayoutSpec(OcflExtensionConfig layoutConfig) {
        try {
            writeSpecFile(layoutConfig.getClass().getClassLoader(), layoutConfig.getExtensionName() + ".md");
        } catch (RuntimeException e) {
            LOG.warn("Failed to write spec file for layout extension {}", layoutConfig.getExtensionName(), e);
        }
    }

    private void writeSpecFile(ClassLoader classLoader, String fileName) {
        try (var stream = classLoader.getResourceAsStream(SPECS_DIR + fileName)) {
            if (stream != null) {
                writeStream(fileName, stream);
            } else {
                throw new OcflJavaException("No spec file found for " + fileName);
            }
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private void writeNamasteFile(OcflVersion ocflVersion) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflVersion());
        storage.write(namasteFile.fileName(),
                namasteFile.fileContent().getBytes(StandardCharsets.UTF_8),
                MEDIA_TYPE_TEXT);
    }

    private void writeOcflLayout(OcflExtensionConfig layoutConfig, String description) {
        var spec = new OcflLayout()
                .setExtension(layoutConfig.getExtensionName())
                .setDescription(description);
        try {
            storage.write(OcflConstants.OCFL_LAYOUT, objectMapper.writeValueAsBytes(spec), MEDIA_TYPE_JSON);
            if (layoutConfig.hasParameters()) {
                storage.createDirectories(FileUtil.pathJoinFailEmpty(OcflConstants.EXTENSIONS_DIR,
                        layoutConfig.getExtensionName()));
                storage.write(layoutConfigFile(layoutConfig.getExtensionName()),
                        objectMapper.writeValueAsBytes(layoutConfig), MEDIA_TYPE_JSON);
            }
        } catch (IOException e) {
            throw new OcflIOException(e);
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

    private OcflLayout readOcflLayout() {
        try (var stream = storage.read(OcflConstants.OCFL_LAYOUT)) {
            return read(stream, OcflLayout.class);
        } catch (OcflNoSuchFileException e) {
            return null;
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private OcflExtensionConfig readLayoutConfig(OcflLayout ocflLayout, Class<? extends OcflExtensionConfig> clazz) {
        try (var stream = storage.read(layoutConfigFile(ocflLayout.getExtension()))) {
            return read(stream, clazz);
        } catch (OcflNoSuchFileException e) {
            // No config found, create default config object
            return initClass(clazz);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private <T> T initClass(Class<T> clazz) {
        try {
            return clazz.getDeclaredConstructor().newInstance();
        } catch (Exception e) {
            throw new RepositoryConfigurationException(String.format("Failed to init OCFL storage layout extension configuration class %s", clazz), e);
        }
    }

    private String layoutConfigFile(String extensionName) {
        return FileUtil.pathJoinFailEmpty(OcflConstants.EXTENSIONS_DIR,
                extensionName,
                OcflConstants.EXT_CONFIG_JSON);
    }

    private <T> T read(InputStream stream, Class<T> clazz) {
        try {
            return objectMapper.readValue(stream, clazz);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private void writeStream(String remotePath, InputStream stream) {
        try {
            storage.write(remotePath, stream.readAllBytes(), MEDIA_TYPE_TEXT);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private List<Listing> list(String path) {
        try {
            return storage.listDirectory(path);
        } catch (OcflNoSuchFileException e) {
            return Collections.emptyList();
        }
    }

    private boolean directoryIsEmpty(String path) {
        try {
            return storage.directoryIsEmpty(path);
        } catch (OcflNoSuchFileException e) {
            return true;
        }
    }

}
