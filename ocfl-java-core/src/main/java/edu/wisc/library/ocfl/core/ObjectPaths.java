package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.model.VersionId;

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
     */
    public static Path inventoryPath(Path directory) {
        return directory.resolve(OcflConstants.INVENTORY_FILE);
    }

    /**
     * Path to an inventory sidecar file within the given directory
     */
    public static Path inventorySidecarPath(Path directory, Inventory inventory) {
        return directory.resolve(OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getOcflName());
    }

    /**
     * Path to an inventory file within the mutable HEAD
     */
    public static Path mutableHeadInventoryPath(Path objectRootPath) {
        return inventoryPath(objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_VERSION_PATH));
    }

    /**
     * Creates an ObjectRoot using absolute paths
     */
    public static ObjectRoot objectRoot(Inventory inventory, Path objectRootPath) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notNull(objectRootPath, "objectRootPath cannot be null");
        return new ObjectRoot(inventory, objectRootPath);
    }

    /**
     * Creates an ObjectRoot with paths relative to the object's root
     */
    public static ObjectRoot objectRoot(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");
        return new ObjectRoot(inventory, null);
    }

    /**
     * Creates a VersionRoot object. This can be used on any valid version directory. There is no requirement for the
     * directory to be located within the object root.
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
            return ObjectPaths.inventoryPath(path);
        }

        @Override
        public Path inventorySidecar() {
            return ObjectPaths.inventorySidecarPath(path, inventory);
        }

        public Path versionPath(VersionId versionId) {
            if (inventory.getHead().equals(versionId)) {
                return headVersionPath();
            }
            return path.resolve(versionId.toString());
        }

        public Path headVersionPath() {
            if (inventory.hasMutableHead()) {
                return mutableHeadPath();
            }
            return path.resolve(inventory.getHead().toString());
        }

        public Path mutableHeadExtensionPath() {
            return path.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH);
        }

        public Path mutableHeadPath() {
            return path.resolve(OcflConstants.MUTABLE_HEAD_VERSION_PATH);
        }

        public VersionRoot version(VersionId versionId) {
            return new VersionRoot(inventory, versionPath(versionId));
        }

        public VersionRoot headVersion() {
            return new VersionRoot(inventory, headVersionPath());
        }

        public VersionRoot mutableHeadVersion() {
            return new VersionRoot(inventory, mutableHeadPath());
        }

    }

    /**
     * Provides methods for navigating an OCFL object version directory
     */
    public static class VersionRoot implements HasInventory {

        private Inventory inventory;
        private Path path;

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
            return ObjectPaths.inventoryPath(path);
        }

        @Override
        public Path inventorySidecar() {
            return ObjectPaths.inventorySidecarPath(path, inventory);
        }

        public Path contentPath() {
            return path.resolve(inventory.resolveContentDirectory());
        }

        public ContentRoot contentRoot() {
            return new ContentRoot(inventory, contentPath());
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
