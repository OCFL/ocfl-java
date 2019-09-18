package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.User;

import java.util.Map;

public class OcflMatchers {

    private OcflMatchers() {

    }

    public static CommitInfoMatcher commitInfo(String name, String address, String message) {
        return new CommitInfoMatcher(name, address, message);
    }

    public static CommitInfoMatcher commitInfo(User user, String message) {
        return new CommitInfoMatcher(user.getName(), user.getAddress(), message);
    }

    public static FileDetailsMatcher fileDetails(String filePath, Map<String, String> fixity) {
        return new FileDetailsMatcher(filePath, fixity);
    }

    public static VersionDetailsMatcher versionDetails(String objectId, String versionId, CommitInfoMatcher commitInfoMatcher, FileDetailsMatcher... fileDetailsMatchers) {
        return new VersionDetailsMatcher(objectId, versionId, commitInfoMatcher, fileDetailsMatchers);
    }

}
