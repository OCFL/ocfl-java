package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryBuilder;
import edu.wisc.library.ocfl.core.model.VersionBuilder;

import java.nio.file.Paths;
import java.time.OffsetDateTime;

/**
 * Converts a mutable HEAD version into a regular OCFL version. This involves rewriting the manifest and fixity fields
 * so that they no longer reference files within the extensions directory.
 */
public class MutableHeadInventoryCommitter {

    /**
     * Converts a mutable HEAD version into a regular OCFL version. This involves rewriting the manifest and fixity fields
     * so that they no longer reference files within the extensions directory.
     *
     * @param original the inventory that contains a mutable head that should be converted
     * @param createdTimestamp the current timestamp
     * @param commitInfo information about the version. Can be null.
     * @return A new inventory with the mutable HEAD version rewritten.
     */
    public Inventory commit(Inventory original, OffsetDateTime createdTimestamp, CommitInfo commitInfo) {
        Enforce.notNull(original, "inventory cannot be null");
        Enforce.notNull(createdTimestamp, "createdTimestamp cannot be null");

        var inventoryBuilder = new InventoryBuilder(original)
                .mutableHead(false)
                .revisionId(null);

        var versionBuilder = new VersionBuilder(original.getHeadVersion())
                .created(createdTimestamp)
                .commitInfo(commitInfo);

        var versionStr = original.getHead().toString();
        var mutableHeadFileIds = original.getFileIdsForMatchingFiles(Paths.get(OcflConstants.MUTABLE_HEAD_VERSION_PATH));

        mutableHeadFileIds.forEach(fileId -> {
            var originalPath = original.getContentPath(fileId);
            var newPath = rewritePath(originalPath, versionStr);
            var digests = original.getFixityForContentPath(originalPath);

            inventoryBuilder.removeContentPath(originalPath);
            inventoryBuilder.addFileToManifest(fileId, newPath);

            if (digests != null) {
                digests.forEach((algorithm, digest) -> {
                    inventoryBuilder.addFixityForFile(newPath, algorithm, digest);
                });
            }
        });

        return inventoryBuilder
                .putVersion(original.getHead(), versionBuilder.build())
                .build();
    }

    private String rewritePath(String path, String version) {
        return path.replace(OcflConstants.MUTABLE_HEAD_VERSION_PATH, version);
    }

}
