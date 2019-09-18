package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.FileDetails;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Objects;

public class FileDetailsMatcher extends TypeSafeMatcher<FileDetails> {

    private String filePath;
    private Map<String, String> fixity;

    FileDetailsMatcher(String filePath, Map<String, String> fixity) {
        this.filePath = filePath;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(FileDetails item) {
        return Objects.equals(filePath, item.getFilePath())
                && Objects.equals(fixity, item.getFixity());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("FileDetails{filePath=")
                .appendValue(filePath)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");

    }

}
