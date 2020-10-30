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

package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.model.FileChange;
import edu.wisc.library.ocfl.api.model.FileChangeHistory;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.ObjectDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.api.model.VersionNum;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to map Inventory objects to API response objects
 */
public class ResponseMapper {

    public ObjectDetails mapInventory(Inventory inventory) {
        var details = new ObjectDetails()
                .setId(inventory.getId())
                .setDigestAlgorithm(inventory.getDigestAlgorithm())
                .setHeadVersionNum(inventory.getHead());

        var versionMap = inventory.getVersions().entrySet().stream()
                .map(entry -> mapVersion(inventory, entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(VersionDetails::getVersionNum, Function.identity()));

        details.setVersions(versionMap);

        return details;
    }

    public VersionDetails mapVersion(Inventory inventory, VersionNum versionNum, Version version) {
        return new VersionDetails()
                .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionNum))
                .setCreated(version.getCreated())
                .setMutable(inventory.hasMutableHead() && inventory.getHead().equals(versionNum))
                .setFileMap(mapFileDetails(inventory, version))
                .setVersionInfo(versionInfo(version));
    }

    private Map<String, FileDetails> mapFileDetails(Inventory inventory, Version version) {
        var fileDetailsMap = new HashMap<String, FileDetails>();

        var digestAlgorithm = inventory.getDigestAlgorithm();

        version.getState().forEach((digest, paths) -> {
            paths.forEach(path -> {
                var contentPath = inventory.getContentPath(digest);
                var details = new FileDetails()
                        .setPath(path)
                        .setStorageRelativePath(inventory.storagePath(digest))
                        .addDigest(digestAlgorithm, digest);

                var digests = inventory.getFixityForContentPath(contentPath);
                digests.forEach(details::addDigest);
                fileDetailsMap.put(path, details);
            });
        });

        return fileDetailsMap;
    }

    public FileChangeHistory fileChangeHistory(Inventory inventory, String logicalPath) {
        var changes = new ArrayList<FileChange>();

        String lastFileId = null;

        for (var entry : inventory.getVersions().entrySet()) {
            var versionNum = entry.getKey();
            var version = entry.getValue();
            var fileId = version.getFileId(logicalPath);

            if (fileId != null && !Objects.equals(lastFileId, fileId)) {
                lastFileId = fileId;
                var contentPath = inventory.ensureContentPath(fileId);
                var fixity = inventory.getFixityForContentPath(contentPath);
                fixity.put(inventory.getDigestAlgorithm(), fileId);

                changes.add(new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionNum))
                        .setPath(logicalPath)
                        .setTimestamp(version.getCreated())
                        .setVersionInfo(versionInfo(version))
                        .setStorageRelativePath(inventory.storagePath(fileId))
                        .setFixity(fixity));
            } else if (fileId == null && lastFileId != null) {
                lastFileId = null;
                changes.add(new FileChange()
                        .setChangeType(FileChangeType.REMOVE)
                        .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionNum))
                        .setPath(logicalPath)
                        .setTimestamp(version.getCreated())
                        .setVersionInfo(versionInfo(version))
                        .setFixity(Collections.emptyMap()));
            }
        }

        return new FileChangeHistory().setPath(logicalPath).setFileChanges(changes);
    }

    private VersionInfo versionInfo(Version version) {
        var versionInfo = new VersionInfo()
                .setMessage(version.getMessage())
                .setCreated(version.getCreated());

        if (version.getUser() != null) {
            versionInfo.setUser(version.getUser().getName(), version.getUser().getAddress());
        }

        return versionInfo;
    }

}
