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

package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.model.VersionNum;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionNum;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Collectors;

/**
 * Centralizes common OCFL path locations
 */
public final class ObjectPaths {

    private ObjectPaths() {

    }

    /**
     * Path to an inventory file within the given directory
     *
     * @param directory parent directory of an inventory file
     * @return path to inventory file
     */
    public static Path inventoryPath(Path directory) {
        return directory.resolve(OcflConstants.INVENTORY_FILE);
    }

    /**
     * Path to an inventory file within the given directory
     *
     * @param directory parent directory of an inventory file
     * @return path to inventory file
     */
    public static String inventoryPath(String directory) {
        return FileUtil.pathJoinFailEmpty(directory, OcflConstants.INVENTORY_FILE);
    }

    /**
     * Path to an inventory sidecar file within the given directory
     *
     * @param directory parent directory of an inventory file
     * @param inventory deserialized inventory
     * @return path to inventory sidecar
     */
    public static Path inventorySidecarPath(Path directory, Inventory inventory) {
        return directory.resolve(OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getOcflName());
    }

    /**
     * Path to an inventory sidecar file within the given directory
     *
     * @param directory parent directory of an inventory file
     * @param inventory deserialized inventory
     * @return path to inventory sidecar
     */
    public static String inventorySidecarPath(String directory, Inventory inventory) {
        return FileUtil.pathJoinFailEmpty(directory, OcflConstants.INVENTORY_SIDECAR_PREFIX + inventory.getDigestAlgorithm().getOcflName());
    }

    /**
     * Path to an inventory sidecar file within the given directory
     *
     * @param directory parent directory of an inventory file
     * @return path to inventory sidecar
     */
    public static Path findInventorySidecarPath(Path directory) {
        return findSidecarPathInternal(directory, OcflConstants.INVENTORY_SIDECAR_PREFIX);
    }

    /**
     * Path to the saved sidecar file within the mutable head extension
     *
     * @param directory parent directory of an inventory file
     * @return path to the saved root inventory sidecar
     */
    public static Path findMutableHeadRootInventorySidecarPath(Path directory) {
        return findSidecarPathInternal(directory, "root-" + OcflConstants.INVENTORY_SIDECAR_PREFIX);
    }

    /**
     * Path to the logs directory within an object
     *
     * @param objectRoot object root directory
     * @return path to logs
     */
    public static Path logsPath(Path objectRoot) {
        return objectRoot.resolve(OcflConstants.LOGS_DIR);
    }

    /**
     * Path to the logs directory within an object
     *
     * @param objectRoot object root directory
     * @return path to logs
     */
    public static String logsPath(String objectRoot) {
        return FileUtil.pathJoinFailEmpty(objectRoot, OcflConstants.LOGS_DIR);
    }

    /**
     * Path to the extensions directory within an object
     *
     * @param objectRoot object root directory
     * @return path to extensions
     */
    public static Path extensionsPath(Path objectRoot) {
        return objectRoot.resolve(OcflConstants.EXTENSIONS_DIR);
    }

    /**
     * Path to the extensions directory within an object
     *
     * @param objectRoot object root directory
     * @return path to extensions
     */
    public static String extensionsPath(String objectRoot) {
        return FileUtil.pathJoinFailEmpty(objectRoot, OcflConstants.EXTENSIONS_DIR);
    }

    /**
     * Path to an inventory file within the mutable HEAD
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD inventory file
     */
    public static Path mutableHeadInventoryPath(Path objectRootPath) {
        return inventoryPath(mutableHeadVersionPath(objectRootPath));
    }

    /**
     * Path to an inventory file within the mutable HEAD
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD inventory file
     */
    public static String mutableHeadInventoryPath(String objectRootPath) {
        return inventoryPath(mutableHeadVersionPath(objectRootPath));
    }

    /**
     * Path to an inventory sidecar file within the given directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @param inventory deserialized inventory
     * @return path to inventory sidecar
     */
    public static String mutableHeadInventorySidecarPath(String objectRootPath, Inventory inventory) {
        return inventorySidecarPath(mutableHeadVersionPath(objectRootPath), inventory);
    }

    /**
     * Path to the mutable HEAD extension version directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD extension version directory
     */
    public static String mutableHeadVersionPath(String objectRootPath) {
        return FileUtil.pathJoinFailEmpty(objectRootPath, OcflConstants.MUTABLE_HEAD_VERSION_PATH);
    }

    /**
     * Path to the mutable HEAD extension version directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD extension version directory
     */
    public static Path mutableHeadVersionPath(Path objectRootPath) {
        return objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_VERSION_PATH);
    }

    /**
     * Path to the mutable HEAD extension root directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD extension root directory
     */
    public static String mutableHeadExtensionRoot(String objectRootPath) {
        return FileUtil.pathJoinFailEmpty(objectRootPath, OcflConstants.MUTABLE_HEAD_EXT_PATH);
    }

    /**
     * Path to the mutable HEAD extension root directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD extension root directory
     */
    public static Path mutableHeadExtensionRoot(Path objectRootPath) {
        return objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH);
    }

    /**
     * Path the revisions directory under the mutable HEAD extension directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return Path the revisions directory under the mutable HEAD extension directory
     */
    public static String mutableHeadRevisionsPath(String objectRootPath) {
        return FileUtil.pathJoinFailEmpty(objectRootPath, OcflConstants.MUTABLE_HEAD_REVISIONS_PATH);
    }

    /**
     * Path the revisions directory under the mutable HEAD extension directory
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return Path the revisions directory under the mutable HEAD extension directory
     */
    public static Path mutableHeadRevisionsPath(Path objectRootPath) {
        return objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_REVISIONS_PATH);
    }

    /**
     * Creates a new ObjectRoot object
     *
     * @param inventory deserialized inventory
     * @return ObjectRoot
     */
    public static ObjectRoot objectRoot(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");
        return new ObjectRoot(inventory);
    }

    private static Path findSidecarPathInternal(Path directory, String prefix) {
        try (var files = Files.list(directory)) {
            var sidecars = files
                    .filter(file -> file.getFileName().toString().startsWith(prefix))
                    .collect(Collectors.toList());

            if (sidecars.size() != 1) {
                throw new CorruptObjectException(String.format("Expected there to be one inventory sidecar file in %s, but found %s.",
                        directory, sidecars.size()));
            }

            return sidecars.get(0);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    public interface HasInventory {
        String inventoryFile();

        String inventorySidecar();
    }

    /**
     * Provides methods for navigating an OCFL object root
     */
    public static class ObjectRoot implements HasInventory {

        private final Inventory inventory;
        private final String path;

        private String inventoryFile;
        private String inventorySidecar;
        private String headVersionPath;
        private String mutableHeadExtPath;
        private String mutableHeadPath;
        private String mutableHeadRevisionsPath;

        private VersionRoot headVersion;
        private VersionRoot mutableHeadVersion;

        private ObjectRoot(Inventory inventory) {
            this.inventory = inventory;
            this.path = inventory.getObjectRootPath();
        }

        public String path() {
            return path;
        }

        public String objectId() {
            return inventory.getId();
        }

        @Override
        public String inventoryFile() {
            if (inventoryFile == null) {
                inventoryFile = ObjectPaths.inventoryPath(path);
            }
            return inventoryFile;
        }

        @Override
        public String inventorySidecar() {
            if (inventorySidecar == null) {
                inventorySidecar = ObjectPaths.inventorySidecarPath(path, inventory);
            }
            return inventorySidecar;
        }

        public String versionPath(VersionNum versionNum) {
            if (inventory.getHead().equals(versionNum)) {
                return headVersionPath();
            }
            return FileUtil.pathJoinIgnoreEmpty(path, versionNum.toString());
        }

        public String headVersionPath() {
            if (headVersionPath == null) {
                if (inventory.hasMutableHead()) {
                    headVersionPath = mutableHeadPath();
                } else {
                    headVersionPath = FileUtil.pathJoinIgnoreEmpty(path, inventory.getHead().toString());
                }
            }
            return headVersionPath;
        }

        public String mutableHeadExtensionPath() {
            if (mutableHeadExtPath == null) {
                mutableHeadExtPath = ObjectPaths.mutableHeadExtensionRoot(path);
            }
            return mutableHeadExtPath;
        }

        public String mutableHeadPath() {
            if (mutableHeadPath == null) {
                mutableHeadPath = ObjectPaths.mutableHeadVersionPath(path);
            }
            return mutableHeadPath;
        }

        public String mutableHeadRevisionsPath() {
            if (mutableHeadRevisionsPath == null) {
                mutableHeadRevisionsPath = ObjectPaths.mutableHeadRevisionsPath(path);
            }
            return mutableHeadRevisionsPath;
        }

        public VersionRoot version(VersionNum versionNum) {
            return new VersionRoot(inventory, versionPath(versionNum));
        }

        public VersionRoot headVersion() {
            if (headVersion == null) {
                headVersion = new VersionRoot(inventory, headVersionPath());
            }
            return headVersion;
        }

        public VersionRoot mutableHeadVersion() {
            if (mutableHeadVersion == null) {
                mutableHeadVersion = new VersionRoot(inventory, mutableHeadPath());
            }
            return mutableHeadVersion;
        }

    }

    /**
     * Provides methods for navigating an OCFL object version directory
     */
    public static class VersionRoot implements HasInventory {

        private final Inventory inventory;
        private final String path;

        private String inventoryFile;
        private String inventorySidecar;
        private String contentPath;

        private ContentRoot contentRoot;

        private VersionRoot(Inventory inventory, String path) {
            this.inventory = inventory;
            this.path = path == null ? "" : path;
        }

        public String objectId() {
            return inventory.getId();
        }

        public String path() {
            return path;
        }

        @Override
        public String inventoryFile() {
            if (inventoryFile == null) {
                inventoryFile = ObjectPaths.inventoryPath(path);
            }
            return inventoryFile;
        }

        @Override
        public String inventorySidecar() {
            if (inventorySidecar == null) {
                inventorySidecar = ObjectPaths.inventorySidecarPath(path, inventory);
            }
            return inventorySidecar;
        }

        public String contentPath() {
            if (contentPath == null) {
                contentPath = FileUtil.pathJoinIgnoreEmpty(path, inventory.resolveContentDirectory());
            }
            return contentPath;
        }

        public ContentRoot contentRoot() {
            if (contentRoot == null) {
                contentRoot = new ContentRoot(inventory, contentPath());
            }
            return contentRoot;
        }

    }

    /**
     * Provides methods for navigating a version's content directory
     */
    public static class ContentRoot {

        private final Inventory inventory;
        private final String path;

        private ContentRoot(Inventory inventory, String path) {
            this.inventory = inventory;
            this.path = path == null ? "" : path;
        }

        public String objectId() {
            return inventory.getId();
        }

        private String path() {
            return path;
        }

        public String revisionPath(RevisionNum revisionNum) {
            return FileUtil.pathJoinIgnoreEmpty(path, revisionNum.toString());
        }

        public String headRevisionPath() {
            if (inventory.getRevisionNum() == null) {
                return null;
            }
            return revisionPath(inventory.getRevisionNum());
        }

    }

}
