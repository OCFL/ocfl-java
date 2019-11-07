package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;

import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Used to map Inventory objects to API response objects
 */
public class ResponseMapper {

    public ObjectDetails mapInventory(Inventory inventory, Path objectRootPath) {
        var details = new ObjectDetails()
                .setId(inventory.getId())
                .setHeadVersionId(inventory.getHead());

        var versionMap = inventory.getVersions().entrySet().stream()
                .map(entry -> mapVersion(inventory, entry.getKey(), entry.getValue(), objectRootPath))
                .collect(Collectors.toMap(VersionDetails::getVersionId, Function.identity()));

        details.setVersions(versionMap);

        return details;
    }

    public VersionDetails mapVersion(Inventory inventory, VersionId versionId, Version version, Path objectRootPath) {
        var details = new VersionDetails()
                .setObjectId(inventory.getId())
                .setVersionId(versionId)
                .setCreated(version.getCreated())
                .setFileMap(mapFileDetails(inventory, version, objectRootPath));

        var commitInfo = new CommitInfo().setMessage(version.getMessage());

        if (version.getUser() != null) {
            commitInfo.setUser(new User()
                    .setName(version.getUser().getName())
                    .setAddress(version.getUser().getAddress()));
        }

        details.setCommitInfo(commitInfo);

        return details;
    }

    // TODO this isn't very efficient
    private Map<String, FileDetails> mapFileDetails(Inventory inventory, Version version, Path objectRootPath) {
        var fileDetailsMap = new HashMap<String, FileDetails>();
        var fileFixityMap = new HashMap<String, FileDetails>();

        var digestAlgorithm = inventory.getDigestAlgorithm();

        version.getState().forEach((digest, paths) -> {
            paths.forEach(path -> {
                var details = new FileDetails()
                        .setPath(path)
                        .setStorageRelativePath(objectRootPath.resolve(inventory.getFilePath(digest)).toString())
                        .addDigest(digestAlgorithm.getOcflName(), digest);
                fileFixityMap.put(digest, details);
                fileDetailsMap.put(path, details);
            });
        });

        inventory.getFixity().forEach((algorithm, digests) -> {
            if (algorithm != digestAlgorithm) {
                digests.forEach((digest, paths) -> {
                    paths.forEach(path -> {
                        var details = fileFixityMap.get(inventory.getFileId(path));
                        if (details != null) {
                            details.addDigest(algorithm.getOcflName(), digest);
                        }
                    });
                });
            }
        });

        return fileDetailsMap;
    }

}
