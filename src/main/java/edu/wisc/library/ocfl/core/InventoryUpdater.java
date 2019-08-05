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

public class InventoryUpdater {

    private static final String INITIAL_VERSION_ID = "v1";

    private Inventory inventory;
    private Version version;
    private DigestAlgorithm digestAlgorithm;
    private Set<DigestAlgorithm> fixityAlgorithms;

    // TODO all of this mutation on the original object is problematic. should be remodeled.

    public static InventoryUpdater newVersionForInsert(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        return new InventoryUpdater(inventory, fixityAlgorithms, createdTimestamp);
    }

    public static InventoryUpdater newVersionForUpdate(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        var updater = new InventoryUpdater(inventory, fixityAlgorithms, createdTimestamp);
        updater.copyOverPreviousVersionState();
        return updater;
    }

    private InventoryUpdater(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms, OffsetDateTime createdTimestamp) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.fixityAlgorithms = fixityAlgorithms != null ? fixityAlgorithms : new HashSet<>();
        this.digestAlgorithm = inventory.getDigestAlgorithm();

        this.version = new Version()
                .setCreated(createdTimestamp);
        inventory.addNewHeadVersion(calculateVersionId(), version);
    }

    public void addCommitInfo(CommitInfo commitInfo) {
        if (commitInfo != null) {
            version.setMessage(commitInfo.getMessage());
            if (commitInfo.getUser() != null) {
                version.setUser(new User(commitInfo.getUser().getName(), commitInfo.getUser().getAddress()));
            }
        }
    }

    public boolean addFile(Path absolutePath, Path objectRelativePath, UpdateOption... updateOptions) {
        var options = new HashSet<>(Arrays.asList(updateOptions));

        var objectRelativePathStr = objectRelativePath.toString();

        if (version.getFileId(objectRelativePathStr) != null) {
            if (options.contains(UpdateOption.OVERWRITE)) {
                version.removePath(objectRelativePathStr);
            } else {
                // TODO modeled exception
                throw new IllegalStateException(String.format("Cannot add file to %s because there is already a file at that location.",
                        objectRelativePathStr));
            }
        }

        var isNew = false;
        var digest = computeDigest(absolutePath, digestAlgorithm);

        // TODO support no-dedup?
        if (!inventory.manifestContainsId(digest)) {
            isNew = true;
            var versionedPath = Paths.get(inventory.getHead().toString(), inventory.getContentDirectory(), objectRelativePathStr);
            inventory.addFileToManifest(digest, versionedPath.toString());

            fixityAlgorithms.forEach(fixityAlgorithm -> {
                var fixityDigest = digest;
                if (fixityAlgorithm != digestAlgorithm) {
                    fixityDigest = computeDigest(absolutePath, fixityAlgorithm);
                }
                inventory.addFixityForFile(versionedPath.toString(), fixityAlgorithm, fixityDigest);
            });
        }

        version.addFile(digest, objectRelativePathStr);
        return isNew;
    }

    public void removeFile(String path) {
        version.removePath(path);
        // TODO fail to remove non-existent file a success?
    }

    public void renameFile(String sourcePath, String destinationPath, UpdateOption... updateOptions) {
        var options = new HashSet<>(Arrays.asList(updateOptions));
        var srcFileId = version.getFileId(sourcePath);

        if (srcFileId == null) {
            throw new IllegalArgumentException(String.format("The following path was not found in object %s: %s",
                    inventory.getId(), sourcePath));
        }

        if (version.getFileId(destinationPath) != null) {
            if (options.contains(UpdateOption.OVERWRITE)) {
                version.removePath(destinationPath);
            } else {
                // TODO modeled exception
                throw new IllegalStateException(String.format("Cannot move %s to %s because there is already a file at that location.",
                        sourcePath, destinationPath));
            }
        }

        version.removePath(sourcePath);
        version.addFile(srcFileId, destinationPath);
    }

    private void copyOverPreviousVersionState() {
        var previousId = inventory.getHead().previousVersionId();
        var previousVersion = inventory.getVersions().get(previousId);
        version.setState(previousVersion.cloneState());
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private VersionId calculateVersionId() {
        var currentVersionId = inventory.getHead();
        if (currentVersionId != null) {
            return currentVersionId.nextVersionId();
        }

        return VersionId.fromValue(INITIAL_VERSION_ID);
    }

}
