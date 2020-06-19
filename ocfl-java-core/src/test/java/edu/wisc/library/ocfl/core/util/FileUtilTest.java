package edu.wisc.library.ocfl.core.util;

import com.google.common.jimfs.Configuration;
import com.google.common.jimfs.Jimfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.io.FileMatchers.anExistingDirectory;
import static org.hamcrest.io.FileMatchers.anExistingFile;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileUtilTest {

    @TempDir
    public Path tempRoot;

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

    @Test
    public void shouldDeleteAllEmptyDirectories() throws IOException {
        Files.createDirectories(tempRoot.resolve("a/b/c/d"));
        Files.createDirectories(tempRoot.resolve("a/d"));
        Files.createDirectories(tempRoot.resolve("a/c"));

        Files.writeString(tempRoot.resolve("a/c/file3"), "file3");

        FileUtil.deleteEmptyDirs(tempRoot);

        assertThat(tempRoot.resolve("a/b/c").toFile(), anExistingDirectory());
        assertThat(tempRoot.resolve("a/c/file3").toFile(), anExistingFile());
        assertThat(tempRoot.resolve("a/b/c/d").toFile(), not(anExistingDirectory()));
        assertThat(tempRoot.resolve("a/d").toFile(), not(anExistingDirectory()));
    }

    @Test
    public void shouldAllChildrenOfRoot() throws IOException {
        Files.createDirectories(tempRoot.resolve("a/a/a/a"));
        Files.createDirectories(tempRoot.resolve("a/b/b"));
        Files.createDirectories(tempRoot.resolve("a/c"));
        Files.createDirectories(tempRoot.resolve("a/d"));

        Files.writeString(tempRoot.resolve("a/a/a/a/file1"), "file1");
        Files.writeString(tempRoot.resolve("a/b/b/file2"), "file2");
        Files.writeString(tempRoot.resolve("a/b/b/file4"), "file4");
        Files.writeString(tempRoot.resolve("a/c/file3"), "file3");

        FileUtil.deleteChildren(tempRoot.resolve("a"));

        assertThat(tempRoot.resolve("a").toFile(), anExistingDirectory());
        assertThat(tempRoot.resolve("a/a").toFile(), not(anExistingDirectory()));
        assertThat(tempRoot.resolve("a/b").toFile(), not(anExistingDirectory()));
        assertThat(tempRoot.resolve("a/c").toFile(), not(anExistingDirectory()));
        assertThat(tempRoot.resolve("a/d").toFile(), not(anExistingDirectory()));
    }

    @Test
    public void shouldMoveDirectoryWithinFilesystemWithRename() throws IOException {
        var srcRoot = tempRoot.resolve("src");
        var dstRoot = tempRoot.resolve("dst");

        Files.createDirectories(srcRoot.resolve("a/b"));
        Files.createDirectories(srcRoot.resolve("a/c/d"));

        Files.writeString(srcRoot.resolve("a/b/file1"), "file1");
        Files.writeString(srcRoot.resolve("a/file2"), "file2");

        FileUtil.moveDirectory(srcRoot, dstRoot);

        assertThat(srcRoot.toFile(), not(anExistingDirectory()));
        assertThat(dstRoot.resolve("a/file2").toFile(), anExistingFile());
        assertThat(dstRoot.resolve("a/b/file1").toFile(), anExistingFile());
        assertThat(dstRoot.resolve("a/c/d").toFile(), anExistingDirectory());
    }

    @Test
    public void moveShouldFailWhenDestinationExists() throws IOException {
        var srcRoot = Files.createDirectories(tempRoot.resolve("src"));
        var dstRoot = Files.createDirectories(tempRoot.resolve("dst"));

        assertThrows(FileAlreadyExistsException.class, () -> {
            FileUtil.moveDirectory(srcRoot, dstRoot);
        });
    }

    @Test
    public void moveShouldFailWhenDestinationParentDoesNotExist() throws IOException {
        var srcRoot = Files.createDirectories(tempRoot.resolve("src"));
        var dstRoot = tempRoot.resolve("dst/blah");

        assertThat(assertThrows(IllegalArgumentException.class, () -> {
            FileUtil.moveDirectory(srcRoot, dstRoot);
        }).getMessage(), containsString("Parent directory of destination must exist"));
    }

    @Test
    public void shouldMoveDirectoryWhenDstEmptyAcrossFilesystem() throws IOException {
        var srcRoot = tempRoot.resolve("src");
        var dstRoot = Paths.get("target/test/FileUtilTest/dst");

        try {
            Files.createDirectories(dstRoot.getParent());

            Files.createDirectories(srcRoot.resolve("a/b"));
            Files.createDirectories(srcRoot.resolve("a/c/d"));

            Files.writeString(srcRoot.resolve("a/b/file1"), "file1");
            Files.writeString(srcRoot.resolve("a/file2"), "file2");

            FileUtil.moveDirectory(srcRoot, dstRoot);

            assertThat(srcRoot.toFile(), not(anExistingDirectory()));
            assertThat(dstRoot.resolve("a/file2").toFile(), anExistingFile());
            assertThat(dstRoot.resolve("a/b/file1").toFile(), anExistingFile());
            assertThat(dstRoot.resolve("a/c/d").toFile(), anExistingDirectory());
        } finally {
            FileUtil.safeDeleteDirectory(Paths.get("target/test/FileUtilTest"));
        }
    }

    @Test
    public void shouldDeleteParentsWhenEmpty() throws IOException {
        var path = Files.createDirectories(tempRoot.resolve("a/b/c/d/e"));

        Files.createDirectories(tempRoot.resolve("a/b/1"));

        FileUtil.deleteDirAndParentsIfEmpty(path);

        assertThat(tempRoot.resolve("a/b").toFile(), anExistingDirectory());
        assertThat(tempRoot.resolve("a/b/1").toFile(), anExistingDirectory());
        assertThat(tempRoot.resolve("a/b/c").toFile(), not(anExistingDirectory()));
    }

    @Test
    public void shouldNotDeleteParentsWhenNotEmpty() throws IOException {
        var path = Files.createDirectories(tempRoot.resolve("a/b/c/d/e"));

        Files.createDirectories(path.resolve("1"));

        FileUtil.deleteDirAndParentsIfEmpty(path);

        assertThat(path.toFile(), anExistingDirectory());
        assertThat(path.resolve("1").toFile(), anExistingDirectory());
    }

}
