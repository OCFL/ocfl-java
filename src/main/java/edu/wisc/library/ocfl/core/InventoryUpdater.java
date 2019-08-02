package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.CommitMessage;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.*;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.HashSet;
import java.util.Set;

public class InventoryUpdater {

    private static final String INITIAL_VERSION_ID = "v1";

    private Inventory inventory;
    private Version version;
    private DigestAlgorithm digestAlgorithm;
    private Set<DigestAlgorithm> fixityAlgorithms;

    // TODO all of this mutation on the original object is problematic. should be remodeled.

    public static InventoryUpdater newVersionForInsert(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms) {
        return new InventoryUpdater(inventory, fixityAlgorithms);
    }

    public static InventoryUpdater newVersionForUpdate(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms) {
        var updater = new InventoryUpdater(inventory, fixityAlgorithms);
        updater.copyOverPreviousVersionState();
        return updater;
    }

    private InventoryUpdater(Inventory inventory, Set<DigestAlgorithm> fixityAlgorithms) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.fixityAlgorithms = fixityAlgorithms != null ? fixityAlgorithms : new HashSet<>();
        this.digestAlgorithm = inventory.getDigestAlgorithm();

        this.version = new Version()
                .setCreated(OffsetDateTime.now(ZoneOffset.UTC));
        inventory.addNewHeadVersion(calculateVersionId(), version);
    }

    public void addCommitMessage(CommitMessage commitMessage) {
        if (commitMessage != null) {
            version.setMessage(commitMessage.getMessage())
                    .setUser(new User(commitMessage.getUser(), commitMessage.getAddress()));
        }
    }

    public boolean addFile(Path absolutePath, Path objectRelativePath) {
        var isNew = false;
        var digest = computeDigest(absolutePath, digestAlgorithm);

        // TODO support no-dedup?
        if (!inventory.manifestContainsId(digest)) {
            isNew = true;
            var versionedPath = Paths.get(inventory.getHead().toString(), inventory.getContentDirectory(), objectRelativePath.toString());
            inventory.addFileToManifest(digest, versionedPath.toString());

            fixityAlgorithms.forEach(fixityAlgorithm -> {
                var fixityDigest = digest;
                if (fixityAlgorithm != digestAlgorithm) {
                    fixityDigest = computeDigest(absolutePath, fixityAlgorithm);
                }
                inventory.addFixityForFile(versionedPath.toString(), fixityAlgorithm, fixityDigest);
            });
        }

        // TODO this overwrites existing files at the path. may need to implement a force flag if this behavior is not desirable
        version.removePath(objectRelativePath.toString());
        version.addFile(digest, objectRelativePath.toString());
        return isNew;
    }

    public void removeFile(String path) {
        version.removePath(path);
        // TODO fail to remove non-existent file a success?
    }

    public void renameFile(String sourcePath, String destinationPath) {
        var srcFileId = version.getFileId(sourcePath);

        if (srcFileId == null) {
            throw new IllegalArgumentException(String.format("The following path was not found in object %s: %s", inventory.getId(), sourcePath));
        }

        version.removePath(sourcePath);
        // TODO change if we want to error on overwrite
        version.removePath(destinationPath);

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
