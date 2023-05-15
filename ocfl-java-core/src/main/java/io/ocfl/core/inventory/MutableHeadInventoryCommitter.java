/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.ocfl.core.inventory;

import io.ocfl.api.OcflConfig;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.InventoryBuilder;
import io.ocfl.core.model.VersionBuilder;
import io.ocfl.core.validation.InventoryValidator;
import java.nio.file.Paths;
import java.time.OffsetDateTime;

/**
 * Converts a mutable HEAD version into a regular OCFL version. This involves rewriting the manifest and fixity fields
 * so that they no longer reference files within the extensions directory.
 */
public final class MutableHeadInventoryCommitter {

    private MutableHeadInventoryCommitter() {}

    /**
     * Converts a mutable HEAD version into a regular OCFL version. This involves rewriting the manifest and fixity fields
     * so that they no longer reference files within the extensions directory.
     *
     * @param original the inventory that contains a mutable head that should be converted
     * @param createdTimestamp the current timestamp
     * @param versionInfo information about the version. Can be null.
     * @param config the default OCFL configuration
     * @return A new inventory with the mutable HEAD version rewritten.
     */
    public static Inventory commit(
            Inventory original, OffsetDateTime createdTimestamp, VersionInfo versionInfo, OcflConfig config) {
        Enforce.notNull(original, "inventory cannot be null");
        Enforce.notNull(createdTimestamp, "createdTimestamp cannot be null");
        Enforce.notNull(config, "config cannot be null");

        var inventoryBuilder = new InventoryBuilder(original).mutableHead(false).revisionNum(null);

        var versionBuilder = new VersionBuilder(original.getHeadVersion())
                .created(createdTimestamp)
                .versionInfo(versionInfo);

        var versionStr = original.getHead().toString();
        var mutableHeadFileIds =
                original.getFileIdsForMatchingFiles(Paths.get(OcflConstants.MUTABLE_HEAD_VERSION_PATH));

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

        if (config.isUpgradeObjectsOnWrite()
                && inventoryBuilder.getType().compareTo(config.getOcflVersion().getInventoryType()) < 0) {
            inventoryBuilder.type(config.getOcflVersion().getInventoryType());
        }

        return InventoryValidator.validateShallow(inventoryBuilder
                .putVersion(original.getHead(), versionBuilder.build())
                .build());
    }

    private static String rewritePath(String path, String version) {
        return path.replace(OcflConstants.MUTABLE_HEAD_VERSION_PATH, version);
    }
}
