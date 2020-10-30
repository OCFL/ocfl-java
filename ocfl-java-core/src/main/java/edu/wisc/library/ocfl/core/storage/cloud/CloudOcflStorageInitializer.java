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

package edu.wisc.library.ocfl.core.storage.cloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.exception.RepositoryConfigurationException;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.OcflExtensionRegistry;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflLayout;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
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
 * does exist, the client configuration is verified and {@link OcflStorageLayoutExtension} is created.
 */
public class CloudOcflStorageInitializer {

    private static final Logger LOG = LoggerFactory.getLogger(CloudOcflStorageInitializer.class);

    private static final String MEDIA_TYPE_TEXT = "text/plain; charset=UTF-8";
    private static final String MEDIA_TYPE_JSON = "application/json; charset=UTF-8";
    private static final String OBJECT_MARKER_PREFIX = "0=ocfl_object";

    private final CloudClient cloudClient;
    private final ObjectMapper objectMapper;

    public CloudOcflStorageInitializer(CloudClient cloudClient, ObjectMapper objectMapper) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
    }

    public OcflStorageLayoutExtension initializeStorage(OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        ensureBucketExists();

        OcflStorageLayoutExtension layoutExtension;

        if (listRootObjects().isEmpty()) {
            layoutExtension = initNewRepo(ocflVersion, layoutConfig);
        } else {
            layoutExtension = validateExistingRepo(ocflVersion, layoutConfig);
        }

        LOG.info("OCFL repository is configured to use OCFL storage layout extension {} implemented by {}",
                layoutExtension.getExtensionName(), layoutExtension.getClass());

        return layoutExtension;
    }

    private OcflStorageLayoutExtension validateExistingRepo(OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        validateOcflVersion(ocflVersion);

        var ocflLayout = readOcflLayout();

        if (ocflLayout == null) {
            LOG.debug("OCFL layout extension not specified");
            return validateLayoutByInspection(layoutConfig);
        }

        LOG.debug("OCFL layout extension: {}", ocflLayout.getExtension());

        return validateLayoutByConfig(ocflLayout, layoutConfig);
    }

    private void validateOcflVersion(OcflVersion ocflVersion) {
        OcflVersion existingOcflVersion = null;

        for (var file : listRootObjects()) {
            var path = file.getPath();
            if (path.startsWith("0=")) {
                existingOcflVersion = OcflVersion.fromOcflVersionString(path.substring(2));
                break;
            }
        }

        if (existingOcflVersion == null) {
            throw new RepositoryConfigurationException("OCFL root is missing its namaste file, eg. 0=ocfl_1.0.");
        } else if (existingOcflVersion != ocflVersion) {
            throw new RepositoryConfigurationException(String.format("OCFL version mismatch. Expected: %s; Found: %s",
                    ocflVersion, existingOcflVersion));
        }
    }

    private OcflStorageLayoutExtension validateLayoutByConfig(OcflLayout ocflLayout, OcflExtensionConfig layoutConfig) {
        var layoutExtension = loadLayoutExtension(ocflLayout.getExtension());
        var expectedConfig = readLayoutConfig(ocflLayout, layoutExtension.getExtensionConfigClass());

        if (layoutConfig != null && !layoutConfig.equals(expectedConfig)) {
            throw new RepositoryConfigurationException(String.format("Storage layout configuration does not match. On disk: %s; Configured: %s",
                    expectedConfig, layoutConfig));
        }

        layoutExtension.init(expectedConfig);
        return layoutExtension;
    }

    private OcflStorageLayoutExtension validateLayoutByInspection(OcflExtensionConfig layoutConfig) {
        if (layoutConfig == null) {
            throw new RepositoryConfigurationException(String.format(
                    "No storage layout configuration is defined in the OCFL repository in bucket %s. Layout must be configured programmatically.",
                    cloudClient.bucket()));
        }

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);

        var objectRoot = identifyRandomObjectRoot("");

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

    private String identifyRandomObjectRoot(String prefix) {
        var response = cloudClient.listDirectory(prefix);

        for (var object : response.getObjects()) {
            if (object.getKeySuffix().startsWith(OBJECT_MARKER_PREFIX)) {
                var path = object.getKey().getPath();
                return (String) path.subSequence(0, path.lastIndexOf('/'));
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
            throw new OcflIOException(e);
        } catch (KeyNotFoundException e) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException(String.format("Missing inventory at %s in bucket %s", inventoryPath, cloudClient.bucket()));
        }
    }

    private OcflStorageLayoutExtension initNewRepo(OcflVersion ocflVersion, OcflExtensionConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null when initializing a new repo");

        LOG.info("Initializing new OCFL repository in the bucket <{}> prefix <{}>", cloudClient.bucket(), cloudClient.prefix());

        var layoutExtension = loadAndInitLayoutExtension(layoutConfig);

        var keys = new ArrayList<String>();

        try {
            keys.add(writeNamasteFile(ocflVersion));
            keys.add(writeOcflSpec(ocflVersion));
            keys.addAll(writeOcflLayout(layoutConfig, layoutExtension.getDescription()));
            return layoutExtension;
        } catch (RuntimeException e) {
            LOG.error("Failed to initialize OCFL repository", e);
            cloudClient.safeDeleteObjects(keys);
            throw e;
        }
    }

    private String writeOcflSpec(OcflVersion ocflVersion) {
        var ocflSpecFile = ocflVersion.getOcflVersion() + ".txt";
        try (var ocflSpecStream = this.getClass().getClassLoader().getResourceAsStream(ocflSpecFile)) {
            return uploadStream(ocflSpecFile, ocflSpecStream).getPath();
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private String writeNamasteFile(OcflVersion ocflVersion) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflVersion());
        return cloudClient.uploadBytes(namasteFile.fileName(), namasteFile.fileContent().getBytes(StandardCharsets.UTF_8),
                MEDIA_TYPE_TEXT).getPath();
    }

    private List<String> writeOcflLayout(OcflExtensionConfig layoutConfig, String description) {
        var keys = new ArrayList<String>();
        var spec = new OcflLayout()
                .setExtension(layoutConfig.getExtensionName())
                .setDescription(description);
        try {
            keys.add(cloudClient.uploadBytes(OcflConstants.OCFL_LAYOUT, objectMapper.writeValueAsBytes(spec), MEDIA_TYPE_JSON).getPath());
            if (layoutConfig.hasParameters()) {
                keys.add(cloudClient.uploadBytes(layoutConfigFile(layoutConfig.getExtensionName()),
                        objectMapper.writeValueAsBytes(layoutConfig), MEDIA_TYPE_JSON).getPath());
            }
            return keys;
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
        try (var stream = cloudClient.downloadStream(OcflConstants.OCFL_LAYOUT)) {
            return read(stream, OcflLayout.class);
        } catch (KeyNotFoundException e) {
            return null;
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private OcflExtensionConfig readLayoutConfig(OcflLayout ocflLayout, Class<? extends OcflExtensionConfig> clazz) {
        try (var stream = cloudClient.downloadStream(layoutConfigFile(ocflLayout.getExtension()))) {
            return read(stream, clazz);
        } catch (KeyNotFoundException e) {
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
        return extensionName + ".json";
    }

    private <T> T read(InputStream stream, Class<T> clazz) {
        try {
            return objectMapper.readValue(stream, clazz);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private void ensureBucketExists() {
        if (!cloudClient.bucketExists()) {
            throw new RepositoryConfigurationException(String.format("Bucket %s does not exist or is not accessible.", cloudClient.bucket()));
        }
    }

    private CloudObjectKey uploadStream(String remotePath, InputStream stream) {
        try {
            return cloudClient.uploadBytes(remotePath, stream.readAllBytes(), MEDIA_TYPE_TEXT);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private List<CloudObjectKey> listRootObjects() {
        return cloudClient.listDirectory("").getObjects().stream()
                .map(ListResult.ObjectListing::getKey)
                .collect(Collectors.toList());
    }

}
