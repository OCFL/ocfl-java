package edu.wisc.library.ocfl.core.util;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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

    @Test
    public void shouldReturnEmptyWhenNoPathParts() {
        assertEquals("", FileUtil.pathJoinIgnoreEmpty());
        assertEquals("", FileUtil.pathJoinIgnoreEmpty(""));
        assertEquals("", FileUtil.pathJoinIgnoreEmpty(null));
        assertEquals("", FileUtil.pathJoinIgnoreEmpty("", ""));
        assertEquals("", FileUtil.pathJoinIgnoreEmpty(null, null));
        assertEquals("", FileUtil.pathJoinIgnoreEmpty("", "/", "//"));
    }

    @Test
    public void shouldJoinPathsWhenNotEmptyPaths() {
        assertEquals("/", FileUtil.pathJoinIgnoreEmpty("/"));
        assertEquals("a", FileUtil.pathJoinIgnoreEmpty("a"));
        assertEquals("a/b/c", FileUtil.pathJoinIgnoreEmpty("a", "b", "c"));
        assertEquals("/a/b/c", FileUtil.pathJoinIgnoreEmpty("/a", "b", "c"));
        assertEquals("/a/b/c", FileUtil.pathJoinIgnoreEmpty("/a", null, "b", "", "c"));
    }

    @Test
    public void shouldTrimPathsWhenHaveLeadingOrTrailingSlashes() {
        assertEquals("/", FileUtil.pathJoinIgnoreEmpty("/"));
        assertEquals("/a", FileUtil.pathJoinIgnoreEmpty("/a"));
        assertEquals("a/b/c/d", FileUtil.pathJoinIgnoreEmpty("a/", "/b", "/c/", "///d///"));
        assertEquals("/a/b", FileUtil.pathJoinIgnoreEmpty("//a//", "/", "///", "b"));
    }

    @Test
    public void shouldThrowExceptionWhenPathPartEmpty() {
        assertThrows(IllegalArgumentException.class, () -> {
            FileUtil.pathJoinFailEmpty("", null);
        });
        assertThrows(IllegalArgumentException.class, () -> {
            FileUtil.pathJoinFailEmpty("/a", null, "b", "c");
        });
        assertThrows(IllegalArgumentException.class, () -> {
            FileUtil.pathJoinFailEmpty("/a", "", "b", "c");
        });
        assertThrows(IllegalArgumentException.class, () -> {
            FileUtil.pathJoinFailEmpty("/a", "//", "b", "c");
        });
    }

}
