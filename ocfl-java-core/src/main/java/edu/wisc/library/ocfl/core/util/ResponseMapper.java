package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to map Inventory objects to API response objects
 */
public class ResponseMapper {

    public ObjectDetails mapInventory(Inventory inventory) {
        var details = new ObjectDetails()
                .setId(inventory.getId())
                .setHeadVersionId(inventory.getHead());

        var versionMap = inventory.getVersions().entrySet().stream()
                .map(entry -> mapVersion(inventory, entry.getKey(), entry.getValue()))
                .collect(Collectors.toMap(VersionDetails::getVersionId, Function.identity()));

        details.setVersions(versionMap);

        return details;
    }

    public VersionDetails mapVersion(Inventory inventory, VersionId versionId, Version version) {
        return new VersionDetails()
                .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionId))
                .setCreated(version.getCreated())
                .setMutable(inventory.hasMutableHead() && inventory.getHead().equals(versionId))
                .setFileMap(mapFileDetails(inventory, version))
                .setCommitInfo(commitInfo(version));
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
            var versionId = entry.getKey();
            var version = entry.getValue();
            var fileId = version.getFileId(logicalPath);

            if (fileId != null && !Objects.equals(lastFileId, fileId)) {
                lastFileId = fileId;
                var contentPath = inventory.ensureContentPath(fileId);
                var fixity = inventory.getFixityForContentPath(contentPath);
                fixity.put(inventory.getDigestAlgorithm(), fileId);

                changes.add(new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionId))
                        .setPath(logicalPath)
                        .setTimestamp(version.getCreated())
                        .setCommitInfo(commitInfo(version))
                        .setStorageRelativePath(inventory.storagePath(fileId))
                        .setFixity(fixity));
            } else if (fileId == null && lastFileId != null) {
                lastFileId = null;
                changes.add(new FileChange()
                        .setChangeType(FileChangeType.REMOVE)
                        .setObjectVersionId(ObjectVersionId.version(inventory.getId(), versionId))
                        .setPath(logicalPath)
                        .setTimestamp(version.getCreated())
                        .setCommitInfo(commitInfo(version))
                        .setFixity(Collections.emptyMap()));
            }
        }

        return new FileChangeHistory().setPath(logicalPath).setFileChanges(changes);
    }

    private CommitInfo commitInfo(Version version) {
        var commitInfo = new CommitInfo().setMessage(version.getMessage());

        if (version.getUser() != null) {
            commitInfo.setUser(new User()
                    .setName(version.getUser().getName())
                    .setAddress(version.getUser().getAddress()));
        }

        return commitInfo;
    }

}
