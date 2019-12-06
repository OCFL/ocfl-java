package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
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

    public static FileDetailsMatcher fileDetails(String filePath, String storagePath, Map<DigestAlgorithm, String> fixity) {
        return new FileDetailsMatcher(filePath, storagePath, fixity);
    }

    public static VersionDetailsMatcher versionDetails(String objectId, String versionId, CommitInfoMatcher commitInfoMatcher, FileDetailsMatcher... fileDetailsMatchers) {
        return new VersionDetailsMatcher(objectId, versionId, commitInfoMatcher, fileDetailsMatchers);
    }

    public static OcflObjectVersionFileMatcher versionFile(String filePath, String storagePath, String content, Map<DigestAlgorithm, String> fixity) {
        return new OcflObjectVersionFileMatcher(filePath, storagePath, content, fixity);
    }

    public static OcflObjectVersionMatcher objectVersion(String objectId, String versionId, CommitInfoMatcher commitInfoMatcher, OcflObjectVersionFileMatcher... fileMatchers) {
        return new OcflObjectVersionMatcher(objectId, versionId, commitInfoMatcher, fileMatchers);
    }

    public static FileChangeMatcher fileChange(FileChangeType changeType, ObjectVersionId objectVersionId, String filePath,
                                               String storagePath, CommitInfoMatcher commitInfoMatcher, Map<DigestAlgorithm, String> fixity) {
        return new FileChangeMatcher(changeType, objectVersionId, filePath, storagePath, commitInfoMatcher, fixity);
    }

}
