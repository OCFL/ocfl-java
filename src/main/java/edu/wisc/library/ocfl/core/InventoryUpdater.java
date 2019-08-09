package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.UpdateOption;
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

public final class InventoryUpdater {

    private VersionId newVersionId;

    private InventoryBuilder inventoryBuilder;
    private VersionBuilder versionBuilder;
    private DigestAlgorithm digestAlgorithm;
    private Set<DigestAlgorithm> fixityAlgorithms;

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

    public static InventoryUpdater newVersionForInsert(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        var inventoryBuilder = new InventoryBuilder(inventory);
        var versionBuilder = new VersionBuilder().created(createdTimestamp);
        return new InventoryUpdater(inventoryBuilder, versionBuilder, fixityAlgorithms);
    }

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

    public void addCommitInfo(CommitInfo commitInfo) {
        versionBuilder.commitInfo(commitInfo);
    }

    public boolean addFile(Path absolutePath, Path objectRelativePath, UpdateOption... updateOptions) {
        var options = new HashSet<>(Arrays.asList(updateOptions));

        var objectRelativePathStr = objectRelativePath.toString();

        if (versionBuilder.getFileId(objectRelativePathStr) != null) {
            if (options.contains(UpdateOption.OVERWRITE)) {
                versionBuilder.removePath(objectRelativePathStr);
            } else {
                // TODO modeled exception
                throw new IllegalStateException(String.format("Cannot add file to %s because there is already a file at that location.",
                        objectRelativePathStr));
            }
        }

        var isNew = false;
        var digest = computeDigest(absolutePath, digestAlgorithm);

        // TODO support no-dedup?
        if (!inventoryBuilder.manifestContainsId(digest)) {
            isNew = true;
            var versionedPath = versionedPath(objectRelativePathStr);
            inventoryBuilder.addFileToManifest(digest, versionedPath);

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

    public void removeFile(String path) {
        versionBuilder.removePath(path);
    }

    public void renameFile(String sourcePath, String destinationPath, UpdateOption... updateOptions) {
        var options = new HashSet<>(Arrays.asList(updateOptions));
        var srcFileId = versionBuilder.getFileId(sourcePath);

        if (srcFileId == null) {
            throw new IllegalArgumentException(String.format("The following path was not found in object %s: %s",
                    inventoryBuilder.getId(), sourcePath));
        }

        if (versionBuilder.getFileId(destinationPath) != null) {
            if (options.contains(UpdateOption.OVERWRITE)) {
                versionBuilder.removePath(destinationPath);
            } else {
                // TODO modeled exception
                throw new IllegalStateException(String.format("Cannot move %s to %s because there is already a file at that location.",
                        sourcePath, destinationPath));
            }
        }

        versionBuilder.removePath(sourcePath);
        versionBuilder.addFile(srcFileId, destinationPath);
    }

    /**
     * This is an extremely dangerous method that could corrupt an object. It is needed to support the case when a file
     * is added to a version and then removed/renamed before the version is committed. It should not be used in any
     * other circumstance.
     *
     * @param path
     */
    public void removeFileFromManifest(String path) {
        inventoryBuilder.removeFileFromManifest(versionedPath(path));
    }

    public Inventory finalizeUpdate() {
        var version = versionBuilder.build();
        inventoryBuilder.addNewHeadVersion(newVersionId, version);
        return inventoryBuilder.build();
    }

    private String versionedPath(String unversionedPath) {
        return Paths.get(newVersionId.toString(), inventoryBuilder.getContentDirectory(), unversionedPath).toString();
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
