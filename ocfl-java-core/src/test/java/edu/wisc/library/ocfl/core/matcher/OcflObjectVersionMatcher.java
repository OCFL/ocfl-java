package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.OcflObjectVersion;
import edu.wisc.library.ocfl.api.model.VersionId;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class OcflObjectVersionMatcher extends TypeSafeMatcher<OcflObjectVersion> {

    private String objectId;
    private VersionId versionId;
    private CommitInfoMatcher commitInfoMatcher;
    private Collection<OcflObjectVersionFileMatcher> fileMatchers;

    OcflObjectVersionMatcher(String objectId, String versionId, CommitInfoMatcher commitInfoMatcher, OcflObjectVersionFileMatcher... fileMatchers) {
        this.objectId = objectId;
        this.versionId = VersionId.fromString(versionId);
        this.commitInfoMatcher = commitInfoMatcher;
        this.fileMatchers = Arrays.asList(fileMatchers);
    }

    @Override
    protected boolean matchesSafely(OcflObjectVersion item) {
        return Objects.equals(objectId, item.getObjectId())
                && Objects.equals(versionId, item.getVersionId())
                && commitInfoMatcher.matches(item.getCommitInfo())
                // Hamcrest has some infuriating overloaded methods...
                && Matchers.containsInAnyOrder((Collection) fileMatchers).matches(item.getFiles());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("VersionDetails{objectId=")
                .appendValue(objectId)
                .appendText(", versionId=")
                .appendValue(versionId)
                .appendText(", commitInfo=")
                .appendDescriptionOf(commitInfoMatcher)
                .appendText(", file=")
                .appendList("[", ",", "]", fileMatchers)
                .appendText("}");
    }

}
