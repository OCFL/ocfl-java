package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.*;
import edu.wisc.library.ocfl.core.path.ContentPathBuilder;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Helper class that's used to accumulate changes within a new version and output an updated inventory. The same InventoryUpdater
 * instance MUST NOT be used more than once.
 * <p>
 * This class is NOT thread safe.
 */
public final class InventoryUpdater {

    private VersionId newVersionId;
    private RevisionId newRevisionId;

    private InventoryBuilder inventoryBuilder;
    private VersionBuilder versionBuilder;
    private ContentPathBuilder contentPathBuilder;
    private DigestAlgorithm digestAlgorithm;
    private Set<DigestAlgorithm> fixityAlgorithms;

    public static InventoryUpdater.Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ContentPathBuilder.Builder contentPathBuilderBuilder;
        private Set<DigestAlgorithm> fixityAlgorithms;
        private InventoryType defaultInventoryType;
        private String defaultContentDirectory;
        private DigestAlgorithm defaultDigestAlgorithm;

        public Builder contentPathBuilderBuilder(ContentPathBuilder.Builder contentPathBuilderBuilder) {
            this.contentPathBuilderBuilder = Enforce.notNull(contentPathBuilderBuilder, "contentPathBuilderBuilder cannot be null");
            return this;
        }

        public Builder fixityAlgorithms(Set<DigestAlgorithm> fixityAlgorithms) {
            this.fixityAlgorithms = fixityAlgorithms;
            return this;
        }

        public Builder defaultInventoryType(InventoryType defaultInventoryType) {
            this.defaultInventoryType = Enforce.notNull(defaultInventoryType, "defaultInventoryType cannot be null");
            return this;
        }

        public Builder defaultContentDirectory(String defaultContentDirectory) {
            this.defaultContentDirectory = Enforce.notBlank(defaultContentDirectory, "defaultContentDirectory cannot be blank");
            return this;
        }

        public Builder defaultDigestAlgorithm(DigestAlgorithm defaultDigestAlgorithm) {
            this.defaultDigestAlgorithm = Enforce.notNull(defaultDigestAlgorithm, "defaultDigestAlgorithm cannot be null");
            return this;
        }

        /**
         * Creates an InventoryUpdater instance for an object that does not have a pre-existing inventory.
         *
         * @param objectId the id of the object
         * @param createdTimestamp the timestamp the new version was created
         */
        public InventoryUpdater newInventory(String objectId, String objectRootPath, OffsetDateTime createdTimestamp) {
            var versionId = VersionId.fromString(OcflConstants.DEFAULT_INITIAL_VERSION_ID);

            var inventoryBuilder = new InventoryBuilder()
                    .id(objectId)
                    .type(defaultInventoryType)
                    .digestAlgorithm(defaultDigestAlgorithm)
                    .contentDirectory(defaultContentDirectory)
                    .objectRootPath(objectRootPath);

            var versionBuilder = new VersionBuilder().created(createdTimestamp);

            var contentPathBuilder = contentPathBuilderBuilder
                    .buildStandardVersion(inventoryBuilder.getObjectRootPath(), inventoryBuilder.getContentDirectory(), versionId);

            return new InventoryUpdater(versionId, null,
                    inventoryBuilder, versionBuilder, contentPathBuilder, fixityAlgorithms);
        }

        /**
         * Creates an InventoryUpdater instance that's used to insert a new version of an existing object WITHOUT copying over
         * the state from the previous version. This should be used when an entire object is being inserted into the repository.
         *
         * @param inventory the original object inventory (will not be mutated)
         * @param createdTimestamp the timestamp the new version was created
         */
        public InventoryUpdater newVersionForInsert(Inventory inventory, OffsetDateTime createdTimestamp) {
            var inventoryBuilder = new InventoryBuilder(inventory);
            var versionBuilder = new VersionBuilder().created(createdTimestamp);
            var versionId = inventoryBuilder.getHead().nextVersionId();
            var contentPathBuilder = contentPathBuilderBuilder
                    .buildStandardVersion(inventory.getObjectRootPath(),inventoryBuilder.getContentDirectory(), versionId);
            return new InventoryUpdater(versionId, null,
                    inventoryBuilder, versionBuilder, contentPathBuilder, fixityAlgorithms);
        }

        /**
         * Creates an InventoryUpdater instance that's used to create a new version of an object by copying forward its current
         * state and then applying changes to it. This should be used when an object is being selectively updated.
         *
         * @param inventory the original object inventory (will not be mutated)
         * @param createdTimestamp the timestamp the new version was created
         */
        public InventoryUpdater newVersionForUpdate(Inventory inventory, OffsetDateTime createdTimestamp) {
            var inventoryBuilder = new InventoryBuilder(inventory);
            var versionBuilder = new VersionBuilder(inventory.getHeadVersion()).created(createdTimestamp);
            var versionId = inventoryBuilder.getHead().nextVersionId();
            var contentPathBuilder = contentPathBuilderBuilder
                    .buildStandardVersion(inventory.getObjectRootPath(),inventoryBuilder.getContentDirectory(), versionId);
            return new InventoryUpdater(versionId, null,
                    inventoryBuilder, versionBuilder, contentPathBuilder, fixityAlgorithms);
        }

        /**
         * Creates an InventoryUpdater instance that's used to create or update the mutable HEAD version of an object.
         *
         * @param inventory the original object inventory (will not be mutated)
         * @param createdTimestamp the timestamp the new version was created
         */
        public InventoryUpdater mutateHead(Inventory inventory, OffsetDateTime createdTimestamp) {
            var inventoryBuilder = new InventoryBuilder(inventory);
            var versionBuilder = new VersionBuilder(inventory.getHeadVersion()).created(createdTimestamp);

            var versionId = inventory.getHead();
            if (!inventory.hasMutableHead()) {
                versionId = versionId.nextVersionId();
            }

            var revisionId = inventory.getRevisionId();
            if (revisionId == null) {
                revisionId = new RevisionId(1);
            } else {
                revisionId = revisionId.nextRevisionId();
            }

            inventoryBuilder.mutableHead(true).revisionId(revisionId);

            var contentPathBuilder = contentPathBuilderBuilder
                    .buildMutableVersion(inventory.getObjectRootPath(),inventoryBuilder.getContentDirectory(), revisionId);

            return new InventoryUpdater(versionId, revisionId,
                    inventoryBuilder, versionBuilder, contentPathBuilder, fixityAlgorithms);
        }

    }

    private InventoryUpdater(VersionId newVersionId, RevisionId newRevisionId, InventoryBuilder inventoryBuilder,
                             VersionBuilder versionBuilder, ContentPathBuilder contentPathBuilder, Set<DigestAlgorithm> fixityAlgorithms) {
        this.newVersionId = Enforce.notNull(newVersionId, "newVersionId cannot be null");
        this.newRevisionId = newRevisionId;
        this.inventoryBuilder = Enforce.notNull(inventoryBuilder, "inventoryBuilder cannot be null");
        this.versionBuilder = Enforce.notNull(versionBuilder, "versionBuilder cannot be null");
        this.contentPathBuilder = Enforce.notNull(contentPathBuilder, "contentPathBuilder cannot be null");
        this.fixityAlgorithms = fixityAlgorithms != null ? fixityAlgorithms : new HashSet<>();
        this.digestAlgorithm = inventoryBuilder.getDigestAlgorithm();
    }

    /**
     * Adds commit information to the new version
     *
     * @param commitInfo
     */
    public void addCommitInfo(CommitInfo commitInfo) {
        versionBuilder.commitInfo(commitInfo);
    }

    /**
     * Adds a file to the version. If a file with the same digest is already in the object it will not be added again, but
     * the path will be recorded.
     *
     * <p>Returns true if the file digest is new to the object and false otherwise
     *
     * @param absolutePath the path to the file on disk
     * @param logicalPath the logical path of the file
     * @param ocflOptions Optional. Use {@code OcflOption.OVERWRITE} to overwrite existing files at the logicalPath
     * @return true if the file digest is new to the object and false otherwise
     * @throws OverwriteException if there is already a file at logicalPath
     */
    public AddFileResult addFile(String digest, Path absolutePath, String logicalPath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));
        var isNew = false;
        var contentPath = contentPath(digest, logicalPath);
        // TODO enforce logicalPath constraints?

        if (versionBuilder.getFileId(logicalPath) != null) {
            if (options.contains(OcflOption.OVERWRITE)) {
                versionBuilder.removePath(logicalPath);
            } else {
                throw new OverwriteException(String.format("Cannot add file to %s because there is already a file at that location.",
                        logicalPath));
            }
        }

        if (!inventoryBuilder.manifestContainsId(digest)) {
            isNew = true;
            inventoryBuilder.addFileToManifest(digest, contentPath);

            // TODO this could be slow, but I suspect that it is unlikely to be used...
            for (var fixityAlgorithm : fixityAlgorithms) {
                var fixityDigest = digest;
                if (!digestAlgorithm.equals(fixityAlgorithm)) {
                    if (fixityAlgorithm.hasJavaStandardName()) {
                        fixityDigest = DigestUtil.computeDigest(fixityAlgorithm, absolutePath);
                    } else {
                        continue;
                    }
                }
                inventoryBuilder.addFixityForFile(contentPath, fixityAlgorithm, fixityDigest);
            }
        }

        versionBuilder.addFile(digest, logicalPath);
        return new AddFileResult(isNew, contentPath, pathUnderContentDir(contentPath));
    }

    /**
     * Removes a file from the version.
     *
     * @param logicalPath the logical path of the file to remove
     */
    public void removeFile(String logicalPath) {
        if (isMutableHead()) {
            var fileId = versionBuilder.getFileId(logicalPath);

            if (fileId != null) {
                versionBuilder.removePath(logicalPath);

                var contentPaths = Set.copyOf(inventoryBuilder.getContentPaths(fileId));

                contentPaths.forEach(contentPath -> {
                    if (contentPath.startsWith(OcflConstants.MUTABLE_HEAD_VERSION_PATH)
                            && !versionBuilder.containsFileId(fileId)) {
                        inventoryBuilder.removeFileFromManifest(contentPath);
                    }
                });
            }
        } else {
            versionBuilder.removePath(logicalPath);
        }
    }

    /**
     * Renames an existing file within a version
     *
     * @param sourcePath logical path of the source file
     * @param destinationPath logical path of the destination file
     * @param ocflOptions Optional. Use {@code OcflOption.OVERWRITE} to overwrite existing files at destinationPath
     * @throws OverwriteException if there is already a file at destinationPath
     */
    public void renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));
        var srcFileId = versionBuilder.getFileId(sourcePath);

        if (srcFileId == null) {
            throw new IllegalArgumentException(String.format("The following path was not found in object %s: %s",
                    inventoryBuilder.getId(), sourcePath));
        }

        if (versionBuilder.getFileId(destinationPath) != null) {
            if (options.contains(OcflOption.OVERWRITE)) {
                versionBuilder.removePath(destinationPath);
            } else {
                throw new OverwriteException(String.format("Cannot move %s to %s because there is already a file at that location.",
                        sourcePath, destinationPath));
            }
        }

        versionBuilder.removePath(sourcePath);
        versionBuilder.addFile(srcFileId, destinationPath);
    }

    /**
     * Reinstates a file that existed in any version of the object into the current version. This is useful when recovering
     * a prior version of a file or adding back a file that was deleted. Both paths are relative the object's root.
     * Use {@code OcflOption.OVERWRITE} to overwrite an existing file at the destinationPath.
     *
     * @param sourceVersionId the version id of the version to reinstate the sourcePath from
     * @param sourcePath the logical path to the file to be reinstated
     * @param destinationPath the logical path to reinstate the file at
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    public void reinstateFile(VersionId sourceVersionId, String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));
        var fileId = inventoryBuilder.getVersionFileId(sourceVersionId, sourcePath);

        if (fileId == null) {
            throw new IllegalArgumentException(String.format("Object %s version %s does not contain a file at %s",
                    inventoryBuilder.getId(), sourceVersionId, sourcePath));
        }

        if (versionBuilder.getFileId(destinationPath) != null) {
            if (options.contains(OcflOption.OVERWRITE)) {
                versionBuilder.removePath(destinationPath);
            } else {
                throw new OverwriteException(String.format("Cannot reinstate %s from version %s to %s because there is already a file at that location.",
                        sourcePath, sourceVersionId, destinationPath));
            }
        }

        versionBuilder.addFile(fileId, destinationPath);
    }

    /**
     * Constructs a new inventory with the new version in it as HEAD.
     *
     * @return the newly constructed inventory
     */
    public Inventory finalizeUpdate() {
        var version = versionBuilder.build();
        inventoryBuilder.addNewHeadVersion(newVersionId, version);
        return inventoryBuilder.build();
    }

    /**
     * Computes the digest of the given file using the inventory's digest algorithm.
     *
     * @param path to the file
     * @return digest
     */
    public String computeDigest(Path path) {
        return DigestUtil.computeDigest(digestAlgorithm, path);
    }

    public DigestAlgorithm digestAlgorithm() {
        return digestAlgorithm;
    }

    private boolean isMutableHead() {
        return newRevisionId != null;
    }

    private String contentPath(String fileId, String logicalPath) {
        var existingPaths = inventoryBuilder.getContentPaths(fileId);
        if (existingPaths.isEmpty()) {
            return contentPathBuilder.fromLogicalPath(logicalPath);
        }
        return existingPaths.iterator().next();
    }

    private String pathUnderContentDir(String contentPath) {
        var contentDirectory = inventoryBuilder.getContentDirectory();
        String prefix;

        if (isMutableHead()) {
            prefix = FileUtil.pathJoinFailEmpty(
                    OcflConstants.MUTABLE_HEAD_VERSION_PATH,
                    contentDirectory);
        } else {
            prefix = FileUtil.pathJoinFailEmpty(
                    newVersionId.toString(),
                    contentDirectory);
        }

        return contentPath.substring(prefix.length() + 1);
    }

    public static class AddFileResult {

        private boolean isNew;
        private String contentPath;
        private String pathUnderContentDir;

        private AddFileResult(boolean isNew, String contentPath, String pathUnderContentDir) {
            this.isNew = isNew;
            this.contentPath = contentPath;
            this.pathUnderContentDir = pathUnderContentDir;
        }

        public boolean isNew() {
            return isNew;
        }

        public String getContentPath() {
            return contentPath;
        }

        public String getPathUnderContentDir() {
            return pathUnderContentDir;
        }

    }

}
