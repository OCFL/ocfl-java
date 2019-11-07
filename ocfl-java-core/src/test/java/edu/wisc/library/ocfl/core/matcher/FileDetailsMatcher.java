package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.model.FileDetails;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Objects;

public class FileDetailsMatcher extends TypeSafeMatcher<FileDetails> {

    private String filePath;
    private String storagePath;
    private Map<String, String> fixity;

    FileDetailsMatcher(String filePath, String storagePath, Map<String, String> fixity) {
        this.filePath = filePath;
        this.storagePath = storagePath;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(FileDetails item) {
        return Objects.equals(filePath, item.getPath())
                && Objects.equals(storagePath, item.getStorageRelativePath())
                && Objects.equals(fixity, item.getFixity());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("FileDetails{filePath=")
                .appendValue(filePath)
                .appendText(", storagePath=")
                .appendValue(storagePath)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");

    }

}
