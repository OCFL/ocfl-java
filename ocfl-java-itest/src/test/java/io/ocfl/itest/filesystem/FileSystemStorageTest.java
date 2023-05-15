package io.ocfl.itest.filesystem;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.ocfl.api.exception.OcflNoSuchFileException;
import io.ocfl.core.storage.common.Listing;
import io.ocfl.core.storage.common.Storage;
import io.ocfl.core.storage.filesystem.FileSystemStorage;
import io.ocfl.core.util.FileUtil;
import io.ocfl.core.util.UncheckedFiles;
import io.ocfl.itest.StorageTest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileSystemStorageTest extends StorageTest {

    @TempDir
    public Path repoRoot;

    @Override
    protected Storage newStorage() {
        return new FileSystemStorage(repoRoot);
    }

    @Test
    public void listDirWhenHasNoChildren() {
        dir("some/dir");

        var listing = storage.listDirectory("some/dir");

        assertTrue(listing.isEmpty());
    }

    @Test
    public void listDirRecursiveWhenHasChildren() {
        file("some/dir/a/f1.txt");
        file("some/dir/b/f2.txt");
        file("some/dir/1/2/f3.txt");
        dir("some/dir/3/4");
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        var listing = storage.listRecursive("some/dir");

        assertThat(
                listing,
                containsInAnyOrder(
                        Listing.file("f1.txt"),
                        Listing.file("f2.txt"),
                        Listing.file("a/f1.txt"),
                        Listing.file("b/f2.txt"),
                        Listing.file("1/2/f3.txt"),
                        Listing.directory("3/4")));
    }

    @Test
    public void deleteEmptyChildrenWhenExists() {
        file("a/b/1.txt");
        dir("a/b/c");
        file("a/d/e/2.txt");
        dir("a/d/f");

        storage.deleteEmptyDirsDown("a");

        assertThat(
                storage.listRecursive(""), containsInAnyOrder(Listing.file("a/b/1.txt"), Listing.file("a/d/e/2.txt")));
    }

    @Test
    public void deleteEmptyChildrenWhenNotExists() {
        file("a/b/1.txt");
        dir("a/b/c");
        file("a/d/e/2.txt");
        dir("a/d/f");

        storage.deleteEmptyDirsDown("z");

        assertThat(
                storage.listRecursive(""),
                containsInAnyOrder(
                        Listing.file("a/b/1.txt"),
                        Listing.directory("a/b/c"),
                        Listing.file("a/d/e/2.txt"),
                        Listing.directory("a/d/f")));
    }

    @Test
    public void deleteEmptyUpWhenExists() {
        file("1/2/3/f1.txt");
        dir("1/4/a");
        dir("1/4/b/c");

        storage.deleteEmptyDirsUp("1/4/b/c");

        assertThat(
                storage.listRecursive(""),
                containsInAnyOrder(Listing.file("1/2/3/f1.txt"), Listing.directory("1/4/a")));
    }

    @Test
    public void deleteEmptyUpWhenNotExists() {
        file("1/2/3/f1.txt");
        dir("1/4/a");
        dir("1/4/b/c");

        storage.deleteEmptyDirsUp("1/4/b/z");

        assertThat(
                storage.listRecursive(""),
                containsInAnyOrder(
                        Listing.file("1/2/3/f1.txt"), Listing.directory("1/4/a"), Listing.directory("1/4/b/c")));
    }

    @Test
    public void listDirRecursiveWhenHasNoChildren() {
        dir("some/dir");

        var listing = storage.listRecursive("some/dir");

        assertTrue(listing.isEmpty());
    }

    // TODO this test does not work with S3 mock
    @Test
    public void failCopyFileInternalWhenSrcNotExists() {
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.copyFileInternal("some/dir/f3.txt", "f3.txt");
        });
    }

    @Test
    public void emptyWhenDirExistsAndHasNoChildren() {
        file("some/dir/a/1.txt");
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        dir("some/dir2");

        assertTrue(storage.directoryIsEmpty("some/dir2"));
    }

    protected void dir(String path) {
        UncheckedFiles.createDirectories(repoRoot.resolve(path));
    }

    protected void file(String path) {
        file(path, "");
    }

    protected void file(String path, String content) {
        dir(FileUtil.parentPath(path));
        try {
            Files.writeString(repoRoot.resolve(path), content);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected String readFile(String path) {
        try {
            return Files.readString(repoRoot.resolve(path));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
