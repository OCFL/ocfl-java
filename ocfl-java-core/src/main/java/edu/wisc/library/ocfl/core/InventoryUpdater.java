package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
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

        return new InventoryUpdater(inventoryBuilder, versionBuilder, fixityAlgorithms);
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
        return new InventoryUpdater(inventoryBuilder, versionBuilder, fixityAlgorithms);
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
        return new InventoryUpdater(inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

    private InventoryUpdater(InventoryBuilder inventoryBuilder, VersionBuilder versionBuilder, Set<DigestAlgorithm> fixityAlgorithms) {
        this.inventoryBuilder = Enforce.notNull(inventoryBuilder, "inventoryBuilder cannot be null");
        this.versionBuilder = Enforce.notNull(versionBuilder, "versionBuilder cannot be null");
        this.fixityAlgorithms = fixityAlgorithms != null ? fixityAlgorithms : new HashSet<>();
        this.digestAlgorithm = inventoryBuilder.getDigestAlgorithm();

        if (inventoryBuilder.getHead() == null) {
            newVersionId = VersionId.fromValue(OcflConstants.DEFAULT_INITIAL_VERSION_ID);
        } else {
            newVersionId = inventoryBuilder.getHead().nextVersionId();
        }
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
     * @param objectRelativePath the path to the file within the object root
     * @param ocflOptions Optional. Use {@code OcflOption.OVERWRITE} to overwrite existing files at objectRelativePath
     * @return true if the file digest is new to the object and false otherwise
     * @throws OverwriteException if there is already a file at objectRelativePath
     */
    public boolean addFile(String digest, Path absolutePath, Path objectRelativePath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));

        var objectRelativePathStr = objectRelativePath.toString();

        if (versionBuilder.getFileId(objectRelativePathStr) != null) {
            if (options.contains(OcflOption.OVERWRITE)) {
                versionBuilder.removePath(objectRelativePathStr);
            } else {
                throw new OverwriteException(String.format("Cannot add file to %s because there is already a file at that location.",
                        objectRelativePathStr));
            }
        }

        var isNew = false;

        if (!inventoryBuilder.manifestContainsId(digest)) {
            isNew = true;
            var versionedPath = versionedPath(objectRelativePathStr);
            inventoryBuilder.addFileToManifest(digest, versionedPath);

            // TODO this could be slow, but I suspect that it is unlikely to be used
            fixityAlgorithms.forEach(fixityAlgorithm -> {
                var fixityDigest = digest;
                if (fixityAlgorithm != digestAlgorithm) {
                    fixityDigest = computeDigest(absolutePath, fixityAlgorithm);
                }
                inventoryBuilder.addFixityForFile(versionedPath, fixityAlgorithm, fixityDigest);
            });
        }

        versionBuilder.addFile(digest, objectRelativePathStr);
        return isNew;
    }

    /**
     * Removes a file from the version.
     *
     * @param path the unversioned object root relative path
     */
    public void removeFile(String path) {
        versionBuilder.removePath(path);
    }

    /**
     * Renames an existing file within a version
     *
     * @param sourcePath unversioned object root relative path to the source file
     * @param destinationPath unversioned object root relative path to the destination file
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
     * @param sourcePath the path to the file to be reinstated relative the object root
     * @param destinationPath the path to reinstate the file to relative the object root
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
     * Removes a file from the manifest. This should only ever happen if a file is added and then removed within the same
     * update block.
     *
     * @param path unversioned object root relative path to the file
     */
    public void removeFileFromManifest(String path) {
        inventoryBuilder.removeFileFromManifest(versionedPath(path));
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
        return computeDigest(path, digestAlgorithm);
    }

    public DigestAlgorithm digestAlgorithm() {
        return digestAlgorithm;
    }

    private String versionedPath(String unversionedPath) {
        return Paths.get(newVersionId.toString(), inventoryBuilder.getContentDirectory(), unversionedPath).toString();
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
