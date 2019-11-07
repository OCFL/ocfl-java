package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.*;
import edu.wisc.library.ocfl.core.util.DigestUtil;

import java.nio.file.Path;
import java.nio.file.Paths;
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
    private DigestAlgorithm digestAlgorithm;
    private Set<DigestAlgorithm> fixityAlgorithms;

    /**
     * Creates an InventoryUpdater instance for an object that does not have a pre-existing inventory.
     *
     * @param objectId the id of the object
     * @param inventoryType the inventory type
     * @param digestAlgorithm the digest algorithm to use for computing file ids
     * @param contentDirectory the directory to store version content in
     * @param fixityAlgorithms the algorithms to use to compute additional fixity information
     * @param createdTimestamp the timestamp the new version was created
     */
    public static InventoryUpdater newInventory(
            String objectId,
            InventoryType inventoryType,
            DigestAlgorithm digestAlgorithm,
            String contentDirectory,
            Set<DigestAlgorithm> fixityAlgorithms,
            OffsetDateTime createdTimestamp) {

        var inventoryBuilder = new InventoryBuilder()
                .id(objectId)
                .type(inventoryType)
                .digestAlgorithm(digestAlgorithm)
                .contentDirectory(contentDirectory);

        var versionBuilder = new VersionBuilder().created(createdTimestamp);
        return new InventoryUpdater(VersionId.fromValue(OcflConstants.DEFAULT_INITIAL_VERSION_ID),
                null, inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

    /**
     * Creates an InventoryUpdater instance that's used to insert a new version of an existing object WITHOUT copying over
     * the state from the previous version. This should be used when an entire object is being inserted into the repository.
     *
     * @param inventory the original object inventory (will not be mutated)
     * @param fixityAlgorithms the algorithms to use to compute additional fixity information
     * @param createdTimestamp the timestamp the new version was created
     */
    public static InventoryUpdater newVersionForInsert(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        var inventoryBuilder = new InventoryBuilder(inventory);
        var versionBuilder = new VersionBuilder().created(createdTimestamp);
        return new InventoryUpdater(inventoryBuilder.getHead().nextVersionId(), null, inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

    /**
     * Creates an InventoryUpdater instance that's used to create a new version of an object by copying forward its current
     * state and then applying changes to it. This should be used when an object is being selectively updated.
     *
     * @param inventory the original object inventory (will not be mutated)
     * @param fixityAlgorithms the algorithms to use to compute additional fixity information
     * @param createdTimestamp the timestamp the new version was created
     */
    public static InventoryUpdater newVersionForUpdate(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        var inventoryBuilder = new InventoryBuilder(inventory);
        var versionBuilder = new VersionBuilder(inventory.getHeadVersion()).created(createdTimestamp);
        return new InventoryUpdater(inventoryBuilder.getHead().nextVersionId(), null, inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

    public static InventoryUpdater mutateHead(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
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

        return new InventoryUpdater(versionId, revisionId, inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

    private InventoryUpdater(VersionId newVersionId, RevisionId newRevisionId, InventoryBuilder inventoryBuilder, VersionBuilder versionBuilder, Set<DigestAlgorithm> fixityAlgorithms) {
        this.newVersionId = Enforce.notNull(newVersionId, "newVersionId cannot be null");
        this.newRevisionId = newRevisionId;
        this.inventoryBuilder = Enforce.notNull(inventoryBuilder, "inventoryBuilder cannot be null");
        this.versionBuilder = Enforce.notNull(versionBuilder, "versionBuilder cannot be null");
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
    public boolean addFile(String digest, Path absolutePath, Path logicalPath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));

        var logicalPathStr = logicalPath.toString();

        if (versionBuilder.getFileId(logicalPathStr) != null) {
            if (options.contains(OcflOption.OVERWRITE)) {
                versionBuilder.removePath(logicalPathStr);
            } else {
                throw new OverwriteException(String.format("Cannot add file to %s because there is already a file at that location.",
                        logicalPathStr));
            }
        }

        var isNew = false;

        if (!inventoryBuilder.manifestContainsId(digest)) {
            isNew = true;
            var contentPath = contentPath(logicalPathStr);
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

        versionBuilder.addFile(digest, logicalPathStr);
        return isNew;
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
                    if (contentPath.startsWith(OcflConstants.MUTABLE_HEAD_VERSION_PATH.toString())
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

    private String contentPath(String logicalPath) {
        if (isMutableHead()) {
            return OcflConstants.MUTABLE_HEAD_VERSION_PATH
                    .resolve(inventoryBuilder.getContentDirectory())
                    .resolve(newRevisionId.toString())
                    .resolve(logicalPath)
                    .toString();
        }

        return Paths.get(newVersionId.toString(), inventoryBuilder.getContentDirectory(), logicalPath).toString();
    }

    private boolean isMutableHead() {
        return newRevisionId != null;
    }

}
