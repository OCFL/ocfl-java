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
import java.util.Set;

public class InventoryUpdater {

    private static final String INITIAL_VERSION_ID = "v1";

    private Inventory inventory;
    private Version version;
    private DigestAlgorithm digestAlgorithm;

    public static InventoryUpdater newVersion(Inventory inventory) {
        return new InventoryUpdater(inventory);
    }

    private InventoryUpdater(Inventory inventory) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
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

    public boolean addFile(Path absolutePath, Path objectRelativePath, Set<DigestAlgorithm> fixityAlgorithms) {
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

        version.addFile(digest, objectRelativePath.toString());
        return isNew;
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
