package edu.wisc.library.ocfl.core.matcher;

import edu.wisc.library.ocfl.api.OcflObjectVersionFile;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.core.itest.ITestHelper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Objects;

public class OcflObjectVersionFileMatcher extends TypeSafeMatcher<OcflObjectVersionFile> {

    private String filePath;
    private String storagePath;
    private String content;
    private Map<String, String> fixity;

    OcflObjectVersionFileMatcher(String filePath, String storagePath, String content, Map<String, String> fixity) {
        this.filePath = filePath;
        this.storagePath = storagePath;
        this.content = content;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(OcflObjectVersionFile item) {
        return Objects.equals(filePath, item.getPath())
                && Objects.equals(storagePath, item.getStorageRelativePath())
                && Objects.equals(fixity, item.getFixity())
                && sameContent(item.getStream());
    }

    private boolean sameContent(FixityCheckInputStream stream) {
        if (content != null) {
            var actual = ITestHelper.inputToString(stream);
            stream.checkFixity();
            return Objects.equals(content, actual);
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("FileDetails{filePath=")
                .appendValue(filePath)
                .appendText(", storagePath=")
                .appendValue(storagePath)
                .appendValue(", content=")
                .appendValue(content)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");

    }

}