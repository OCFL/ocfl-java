package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;

import java.nio.file.Path;
import java.nio.file.Paths;

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
     * Path to an inventory file within the mutable HEAD
     *
     * @param objectRootPath path to the root of an ocfl object
     * @return path to the mutable HEAD inventory file
     */
    public static Path mutableHeadInventoryPath(Path objectRootPath) {
        return inventoryPath(objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_VERSION_PATH));
    }

    /**
     * Creates an ObjectRoot using absolute paths
     *
     * @param inventory deserialized inventory
     * @param objectRootPath path to the root of an ocfl object
     * @return ObjectRoot
     */
    public static ObjectRoot objectRoot(Inventory inventory, Path objectRootPath) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notNull(objectRootPath, "objectRootPath cannot be null");
        return new ObjectRoot(inventory, objectRootPath);
    }

    /**
     * Creates an ObjectRoot with paths relative to the object's root
     *
     * @param inventory deserialized inventory
     * @return ObjectRoot
     */
    public static ObjectRoot objectRoot(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");
        return new ObjectRoot(inventory, null);
    }

    /**
     * Creates a VersionRoot object. This can be used on any valid version directory. There is no requirement for the
     * directory to be located within the object root.
     *
     * @param inventory deserialized inventory
     * @param location path to the root of the version
     * @return VersionRoot
     */
    public static VersionRoot version(Inventory inventory, Path location) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notNull(location, "location cannot be null");
        return new VersionRoot(inventory, location);
    }

    public interface HasInventory {
        Path inventoryFile();
        Path inventorySidecar();
    }

    /**
     * Provides methods for navigating an OCFL object root
     */
    public static class ObjectRoot implements HasInventory {

        private Inventory inventory;
        private Path path;

        private Path inventoryFile;
        private Path inventorySidecar;
        private Path headVersionPath;
        private Path mutableHeadExtPath;
        private Path mutableHeadPath;

        private VersionRoot headVersion;
        private VersionRoot mutableHeadVersion;

        private ObjectRoot(Inventory inventory, Path path) {
            this.inventory = inventory;
            this.path = path == null ? Paths.get("") : path;
        }

        public Path path() {
            return path;
        }

        public String objectId() {
            return inventory.getId();
        }

        @Override
        public Path inventoryFile() {
            if (inventoryFile == null) {
                inventoryFile = ObjectPaths.inventoryPath(path);
            }
            return inventoryFile;
        }

        @Override
        public Path inventorySidecar() {
            if (inventorySidecar == null) {
                inventorySidecar = ObjectPaths.inventorySidecarPath(path, inventory);
            }
            return inventorySidecar;
        }

        public Path versionPath(VersionId versionId) {
            if (inventory.getHead().equals(versionId)) {
                return headVersionPath();
            }
            return path.resolve(versionId.toString());
        }

        public Path headVersionPath() {
            if (headVersionPath == null) {
                if (inventory.hasMutableHead()) {
                    headVersionPath = mutableHeadPath();
                } else {
                    headVersionPath = path.resolve(inventory.getHead().toString());
                }
            }
            return headVersionPath;
        }

        public Path mutableHeadExtensionPath() {
            if (mutableHeadExtPath == null) {
                mutableHeadExtPath = path.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH);
            }
            return mutableHeadExtPath;
        }

        public Path mutableHeadPath() {
            if (mutableHeadPath == null) {
                mutableHeadPath = path.resolve(OcflConstants.MUTABLE_HEAD_VERSION_PATH);
            }
            return mutableHeadPath;
        }

        public VersionRoot version(VersionId versionId) {
            return new VersionRoot(inventory, versionPath(versionId));
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

        private Inventory inventory;
        private Path path;

        private Path inventoryFile;
        private Path inventorySidecar;
        private Path contentPath;

        private ContentRoot contentRoot;

        private VersionRoot(Inventory inventory, Path path) {
            this.inventory = inventory;
            this.path = path == null ? Paths.get("") : path;
        }

        public String objectId() {
            return inventory.getId();
        }

        public Path path() {
            return path;
        }

        @Override
        public Path inventoryFile() {
            if (inventoryFile == null) {
                inventoryFile = ObjectPaths.inventoryPath(path);
            }
            return inventoryFile;
        }

        @Override
        public Path inventorySidecar() {
            if (inventorySidecar == null) {
                inventorySidecar = ObjectPaths.inventorySidecarPath(path, inventory);
            }
            return inventorySidecar;
        }

        public Path contentPath() {
            if (contentPath == null) {
                contentPath = path.resolve(inventory.resolveContentDirectory());
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

        private Inventory inventory;
        private Path path;

        private ContentRoot(Inventory inventory, Path path) {
            this.inventory = inventory;
            this.path = path == null ? Paths.get("") : path;
        }

        public String objectId() {
            return inventory.getId();
        }

        private Path path() {
            return path;
        }

        public Path revisionPath(RevisionId revisionId) {
            return path.resolve(revisionId.toString());
        }

        public Path headRevisionPath() {
            if (inventory.getRevisionId() == null) {
                return null;
            }
            return revisionPath(inventory.getRevisionId());
        }

    }

}
