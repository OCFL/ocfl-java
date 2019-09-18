package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class VersionDetailsMatcher extends TypeSafeMatcher<VersionDetails> {

    private String objectId;
    private String versionId;
    private CommitInfoMatcher commitInfoMatcher;
    private Collection<Matcher<FileDetails>> fileDetailsMatchers;

    VersionDetailsMatcher(String objectId, String versionId, CommitInfoMatcher commitInfoMatcher, FileDetailsMatcher... fileDetailsMatchers) {
        this.objectId = objectId;
        this.versionId = versionId;
        this.commitInfoMatcher = commitInfoMatcher;
        this.fileDetailsMatchers = Arrays.asList(fileDetailsMatchers);
    }

    @Override
    protected boolean matchesSafely(VersionDetails item) {
        return Objects.equals(objectId, item.getObjectId())
                && Objects.equals(versionId, item.getVersionId())
                && commitInfoMatcher.matches(item.getCommitInfo())
                // Hamcrest has some infuriating overloaded methods...
                && Matchers.containsInAnyOrder((Collection) fileDetailsMatchers).matches(item.getFiles());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("VersionDetails{objectId=")
                .appendValue(objectId)
                .appendText(", versionId=")
                .appendValue(versionId)
                .appendText(", commitInfo=")
                .appendDescriptionOf(commitInfoMatcher)
                .appendText(", fileDetails=")
                .appendList("[", ",", "]", fileDetailsMatchers)
                .appendText("}");
    }

}
