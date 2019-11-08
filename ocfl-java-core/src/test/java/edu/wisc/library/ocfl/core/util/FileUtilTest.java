package edu.wisc.library.ocfl.core.util;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FileUtilTest {

    @Test
    public void shouldDoNothingWhenPathContainsNoSeparators() {
        var fs = Jimfs.newFileSystem(Configuration.unix());
        var path = "asdf";
        assertEquals(path, FileUtil.pathToStringStandardSeparator(fs.getPath(path)));
    }

    @Test
    public void shouldDoNothingWhenPathContainsBackslashAndSystemSeparatorIsForwardSlash() {
        var fs = Jimfs.newFileSystem(Configuration.unix());
        var path = "dir1/file\\with\\backslash";
        assertEquals(path, FileUtil.pathToStringStandardSeparator(fs.getPath(path)));
    }

    @Test
    public void shouldReplaceBackslashesWhenSystemSeparatorIsBackslash() {
        var fs = Jimfs.newFileSystem(Configuration.windows());
        var path = "dir1/file\\with\\backslash";
        assertEquals("dir1/file/with/backslash", FileUtil.pathToStringStandardSeparator(fs.getPath(path)));
    }

}
