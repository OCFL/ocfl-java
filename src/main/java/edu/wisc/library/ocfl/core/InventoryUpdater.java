package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.CommitMessage;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.User;
import edu.wisc.library.ocfl.core.model.Version;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Set;

public class InventoryUpdater {

    // TODO support configuring 0-padding
    private static final String INITIAL_VERSION_ID = "v1";
    private static final String VERSION_PREFIX = "v";

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
            var versionedPath = Paths.get(inventory.getHead(), inventory.getContentDirectory(), objectRelativePath.toString());
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

    private String calculateVersionId() {
        var currentVersionId = inventory.getHead();
        if (currentVersionId != null) {
            return VERSION_PREFIX + (parseVersionNumber(currentVersionId) + 1);
        }

        return INITIAL_VERSION_ID;
    }

    private int parseVersionNumber(String versionId) {
        return Integer.valueOf(versionId.substring(1));
    }

}
