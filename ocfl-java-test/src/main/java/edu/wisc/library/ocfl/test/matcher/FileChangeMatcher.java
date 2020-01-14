package edu.wisc.library.ocfl.test.matcher;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileChange;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Objects;

public class FileChangeMatcher extends TypeSafeMatcher<FileChange> {

    private FileChangeType changeType;
    private ObjectVersionId objectVersionId;
    private String filePath;
    private String storagePath;
    private Map<DigestAlgorithm, String> fixity;

    private CommitInfoMatcher commitInfoMatcher;

    FileChangeMatcher(FileChangeType changeType, ObjectVersionId objectVersionId, String filePath,
                      String storagePath, CommitInfoMatcher commitInfoMatcher, Map<DigestAlgorithm, String> fixity) {
        this.changeType = changeType;
        this.objectVersionId = objectVersionId;
        this.filePath = filePath;
        this.storagePath = storagePath;
        this.commitInfoMatcher = commitInfoMatcher;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(FileChange item) {
        return Objects.equals(filePath, item.getPath())
                && Objects.equals(changeType, item.getChangeType())
                && Objects.equals(objectVersionId, item.getObjectVersionId())
                && Objects.equals(storagePath, item.getStorageRelativePath())
                && commitInfoMatcher.matches(item.getCommitInfo())
                && Objects.equals(fixity, item.getFixity());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("FileChange{filePath=")
                .appendValue(filePath)
                .appendText(", storagePath=")
                .appendValue(storagePath)
                .appendText(", changeType=")
                .appendValue(changeType)
                .appendText(", objectVersionId=")
                .appendValue(objectVersionId)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");

    }

}
