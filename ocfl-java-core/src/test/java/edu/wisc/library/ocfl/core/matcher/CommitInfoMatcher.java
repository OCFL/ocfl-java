package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Objects;

public class CommitInfoMatcher extends TypeSafeMatcher<CommitInfo> {

    private String name;
    private String address;
    private String message;

    CommitInfoMatcher(String name, String address, String message) {
        this.name = name;
        this.address = address;
        this.message = message;
    }

    @Override
    protected boolean matchesSafely(CommitInfo item) {
        var matches = Objects.equals(item.getMessage(), message);

        if (item.getUser() != null) {
            matches &= Objects.equals(item.getUser().getName(), name)
                    && Objects.equals(item.getUser().getAddress(), address);
        } else {
            matches &= name == null && address == null;
        }

        return matches;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("CommitInfo{message=")
                .appendValue(message)
                .appendText(", user={name=")
                .appendValue(name)
                .appendText(", address=")
                .appendValue(address)
                .appendText("}}");
    }
}
