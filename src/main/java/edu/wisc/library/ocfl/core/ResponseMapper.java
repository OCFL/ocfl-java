package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.HashSet;
import java.util.function.Function;
import java.util.stream.Collectors;

public class ResponseMapper {

    public ObjectDetails map(Inventory inventory) {
        var details = new ObjectDetails()
                .setId(inventory.getId())
                .setHeadVersionId(inventory.getHead().toString());

        var versionMap = inventory.getVersions().entrySet().stream()
                .map(entry -> map(ObjectId.version(inventory.getId(), entry.getKey().toString()), entry.getValue()))
                .collect(Collectors.toMap(VersionDetails::getVersionId, Function.identity()));

        details.setVersions(versionMap);

        return details;
    }

    public VersionDetails map(ObjectId objectId, Version version) {
        var details = new VersionDetails()
                .setObjectId(objectId.getObjectId())
                .setVersionId(objectId.getVersionId())
                .setCreated(version.getCreated())
                .setFiles(new HashSet<>(version.listPaths()));

        var commitInfo = new CommitInfo().setMessage(version.getMessage());

        if (version.getUser() != null) {
            commitInfo.setUser(new User()
                    .setName(version.getUser().getName())
                    .setAddress(version.getUser().getAddress()));
        }

        details.setCommitInfo(commitInfo);

        return details;
    }

}
