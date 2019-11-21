package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.HashMap;
import java.util.Map;
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
        var details = new VersionDetails()
                .setObjectId(inventory.getId())
                .setVersionId(versionId)
                .setCreated(version.getCreated())
                .setMutable(inventory.hasMutableHead() && inventory.getHead().equals(versionId))
                .setFileMap(mapFileDetails(inventory, version));

        var commitInfo = new CommitInfo().setMessage(version.getMessage());

        if (version.getUser() != null) {
            commitInfo.setUser(new User()
                    .setName(version.getUser().getName())
                    .setAddress(version.getUser().getAddress()));
        }

        details.setCommitInfo(commitInfo);

        return details;
    }

    private Map<String, FileDetails> mapFileDetails(Inventory inventory, Version version) {
        var objectRootPath = inventory.getObjectRootPath();
        var fileDetailsMap = new HashMap<String, FileDetails>();

        var digestAlgorithm = inventory.getDigestAlgorithm();

        version.getState().forEach((digest, paths) -> {
            paths.forEach(path -> {
                var contentPath = inventory.getContentPath(digest);
                var details = new FileDetails()
                        .setPath(path)
                        .setStorageRelativePath(
                                FileUtil.pathJoinFailEmpty(objectRootPath, contentPath))
                        .addDigest(digestAlgorithm.getOcflName(), digest);

                var digests = inventory.getFixityForContentPath(contentPath);

                digests.forEach((algorithm, value) -> {
                    details.addDigest(algorithm.getOcflName(), value);
                });

                fileDetailsMap.put(path, details);
            });
        });

        return fileDetailsMap;
    }

}
