package edu.wisc.library.ocfl.itest;

import edu.wisc.library.ocfl.api.exception.OcflFileAlreadyExistsException;
import edu.wisc.library.ocfl.api.exception.OcflNoSuchFileException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.storage.common.Listing;
import edu.wisc.library.ocfl.core.storage.common.Storage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class StorageTest {

    @TempDir
    public Path tempRoot;

    protected Storage storage;
    protected Path staging;

    protected abstract Storage newStorage();

    protected abstract void dir(String path);

    protected abstract void file(String path);

    protected abstract void file(String path, String content);

    protected abstract String readFile(String path);

    @BeforeEach
    public void setup() throws IOException {
        staging = Files.createDirectories(tempRoot.resolve("staging"));
        storage = newStorage();
    }

    @Test
    public void listDirWhenHasChildren() {
        file("some/dir/a/1.txt");
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        var listing = storage.listDirectory("some/dir");

        assertThat(listing, containsInAnyOrder(
                Listing.file("f1.txt"),
                Listing.file("f2.txt"),
                Listing.directory("a")));
    }

    @Test
    public void listDirWhenDoesNotExist() {
        dir("some/dir");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.listDirectory("not-here");
        });
    }

    @Test
    public void listDirRecursiveWhenHasChildren() {
        file("some/dir/a/f1.txt");
        file("some/dir/b/f2.txt");
        file("some/dir/1/2/f3.txt");
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        var listing = storage.listRecursive("some/dir");

        assertThat(listing, containsInAnyOrder(
                Listing.file("f1.txt"),
                Listing.file("f2.txt"),
                Listing.file("a/f1.txt"),
                Listing.file("b/f2.txt"),
                Listing.file("1/2/f3.txt")));
    }

    @Test
    public void listDirRecursiveWhenDoesNotExist() {
        dir("some/dir");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.listRecursive("not-here");
        });
    }

    @Test
    public void notEmptyWhenDirExistsAndHasChildren() {
        file("some/dir/a/1.txt");
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        assertFalse(storage.directoryIsEmpty("some"));
        assertFalse(storage.directoryIsEmpty("some/dir"));
    }

    @Test
    public void exceptionWhenDirNotExists() {
        dir("some/dir");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.directoryIsEmpty("not-here");
        });
    }

    @Test
    public void returnTrueWhenFileExists() {
        file("some/dir/f1.txt");
        assertTrue(storage.fileExists("some/dir/f1.txt"));
    }

    @Test
    public void returnFalseWhenFileNotExists() {
        file("some/dir/f1.txt");
        assertFalse(storage.fileExists("bogus/dir/f1.txt"));
    }

    @Test
    public void readFileContentWhenExists() throws IOException {
        var content = "something1";
        file("f1.txt", content);

        try (var stream = storage.read("f1.txt")) {
            assertEquals(content, toString(stream));
        }
    }

    @Test
    public void failReadFileContentWhenNotExists() {
        var content = "something1";
        file("f1.txt", content);

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.read("f2.txt");
        });
    }

    @Test
    public void readFileStringContentWhenExists() {
        var content = "something2";
        file("f1.txt", content);

        assertEquals(content, storage.readToString("f1.txt"));
    }

    @Test
    public void failReadFileStringContentWhenNotExists() {
        var content = "something2";
        file("f1.txt", content);

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.readToString("f2.txt");
        });
    }

    @Test
    public void readFileLazyContentWhenExists() throws IOException {
        var content = "something3";
        file("f1.txt", content);

        var retriever = storage.readLazy("f1.txt", DigestAlgorithm.md5, "f57c22367d47ee55c920465e8f17dc70");

        try (var stream = retriever.retrieveFile()) {
            assertEquals(content, toString(stream));
        }
    }

    @Test
    public void doNotFailReadFileLazyContentWhenNotExists() {
        var content = "something3";
        file("f1.txt", content);
        storage.readLazy("f2.txt", DigestAlgorithm.md5, "f57c22367d47ee55c920465e8f17dc70");
    }

    @Test
    public void writeFileWhenDoesNotExist() {
        var content = "content";

        storage.write("f1.txt", content.getBytes(StandardCharsets.UTF_8), null);

        assertEquals(content, readFile("f1.txt"));
    }

    @Test
    public void writeFileFailWhenDoesExist() {
        file("f1.txt");

        assertThrows(OcflFileAlreadyExistsException.class, () -> {
            storage.write("f1.txt", "content".getBytes(StandardCharsets.UTF_8), null);
        });
    }

    @Test
    public void copyDirectoryOutWhenExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        storage.copyDirectoryOutOf("some", staging);

        assertThat(listRecursive(staging), containsInAnyOrder(
                Listing.file("dir/f1.txt"),
                Listing.file("dir/f2.txt")
        ));
    }

    @Test
    public void failCopyDirectoryOutWhenNotExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.copyDirectoryOutOf("another", staging);
        });
    }

    @Test
    public void copyFileIntoWhenNotExists() throws IOException {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        var file = Files.writeString(staging.resolve("f3.txt"), "f3");

        storage.copyFileInto(file, "some/dir/f3.txt", null);

        assertThat(storage.listDirectory("some/dir"), containsInAnyOrder(
                Listing.file("f1.txt"),
                Listing.file("f2.txt"),
                Listing.file("f3.txt")));
    }

    @Test
    public void copyFileIntoWhenExists() throws IOException {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");

        var file = Files.writeString(staging.resolve("f3.txt"), "f3");

        storage.copyFileInto(file, "some/dir/f1.txt", null);

        assertThat(storage.listDirectory("some/dir"), containsInAnyOrder(
                Listing.file("f1.txt"),
                Listing.file("f2.txt")));

        assertEquals("f3", storage.readToString("some/dir/f1.txt"));
    }

    @Test
    public void copyFileInternalWhenSrcExistsAndDstNotExists() {
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        storage.copyFileInternal("some/dir/f2.txt", "f2.txt");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("f2.txt"),
                Listing.file("some/dir/f1.txt"),
                Listing.file("some/dir/f2.txt")));

        assertEquals("f2", storage.readToString("f2.txt"));
    }

    @Test
    public void copyFileInternalWhenSrcExistsAndDstExists() {
        file("f2.txt", "asdf");
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        storage.copyFileInternal("some/dir/f2.txt", "f2.txt");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("f2.txt"),
                Listing.file("some/dir/f1.txt"),
                Listing.file("some/dir/f2.txt")));

        assertEquals("f2", storage.readToString("f2.txt"));
    }

    @Test
    public void moveDirIntoWhenNotExists() throws IOException {
        Files.createDirectories(staging.resolve("a/b"));
        Files.writeString(staging.resolve("a/f1.txt"), "f1");
        Files.writeString(staging.resolve("a/b/f2.txt"), "f2");

        storage.moveDirectoryInto(staging.resolve("a"), "d");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("d/f1.txt"),
                Listing.file("d/b/f2.txt")));

        assertEquals("f1", storage.readToString("d/f1.txt"));
        assertEquals("f2", storage.readToString("d/b/f2.txt"));
    }

    @Test
    public void failMoveDirIntoWhenExists() throws IOException {
        file("d/f3.txt");

        Files.createDirectories(staging.resolve("a/b"));
        Files.writeString(staging.resolve("a/f1.txt"), "f1");
        Files.writeString(staging.resolve("a/b/f2.txt"), "f2");

        assertThrows(OcflFileAlreadyExistsException.class, () -> {
            storage.moveDirectoryInto(staging.resolve("a"), "d");
        });
    }

    @Test
    public void moveDirInternalWhenSrcExistsAndDstNotExists() {
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        storage.moveDirectoryInternal("some/dir", "another");

        assertThat(storage.listRecursive("another"), containsInAnyOrder(
                Listing.file("f1.txt"),
                Listing.file("f2.txt")));

        assertEquals("f1", storage.readToString("another/f1.txt"));
        assertEquals("f2", storage.readToString("another/f2.txt"));
    }

    @Test
    public void failMoveDirInternalWhenSrcNotExists() {
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        assertThrows(OcflNoSuchFileException.class, () -> {
            storage.moveDirectoryInternal("some/bogus", "another");
        });
    }

    @Test
    public void failMoveDirInternalWhenDstExists() {
        file("some/dir/f1.txt", "f1");
        file("some/dir/f2.txt", "f2");

        assertThrows(OcflFileAlreadyExistsException.class, () -> {
            storage.moveDirectoryInternal("some/dir", "some");
        });
    }

    @Test
    public void deleteDirWhenExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("another/f3.txt");

        storage.deleteDirectory("some");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("another/f3.txt")));
    }

    @Test
    public void deleteDirWhenNotExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("another/f3.txt");

        storage.deleteDirectory("bogus");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("some/dir/f1.txt"),
                Listing.file("some/dir/f2.txt"),
                Listing.file("another/f3.txt")
        ));
    }

    @Test
    public void deleteFileWhenExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("another/f3.txt");

        storage.deleteFile("some/dir/f2.txt");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("some/dir/f1.txt"),
                Listing.file("another/f3.txt")
        ));
    }

    @Test
    public void deleteFileWhenNotExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("another/f3.txt");

        storage.deleteFile("some/dir/f3.txt");

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("some/dir/f1.txt"),
                Listing.file("some/dir/f2.txt"),
                Listing.file("another/f3.txt")
        ));
    }

    @Test
    public void deleteFilesWhenExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("some/dir/f3.txt");

        storage.deleteFiles(List.of("some/dir/f2.txt", "some/dir/f3.txt"));

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("some/dir/f1.txt")
        ));
    }

    @Test
    public void deleteFilesWhenNotExists() {
        file("some/dir/f1.txt");
        file("some/dir/f2.txt");
        file("some/dir/f3.txt");

        storage.deleteFiles(List.of("another/dir/f2.txt", "another/dir/f3.txt"));

        assertThat(storage.listRecursive(""), containsInAnyOrder(
                Listing.file("some/dir/f1.txt"),
                Listing.file("some/dir/f2.txt"),
                Listing.file("some/dir/f3.txt")
        ));
    }

    protected String toString(InputStream stream) {
        try {
            return new String(stream.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected List<Listing> listRecursive(Path path) {
        var listings = new ArrayList<Listing>();

        try {
            Files.walkFileTree(path, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (attrs.isRegularFile()) {
                        listings.add(createListing(Listing.Type.File, file));
                    } else {
                        listings.add(createListing(Listing.Type.Other, file));
                    }
                    return super.visitFile(file, attrs);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    if (FileUtil.isDirEmpty(dir) && !dir.equals(path)) {
                        listings.add(createListing(Listing.Type.Directory, dir));
                    }
                    return super.postVisitDirectory(dir, exc);
                }

                private Listing createListing(Listing.Type type, Path file) {
                    var relative = FileUtil.pathToStringStandardSeparator(path.relativize(file));
                    return new Listing(type, relative);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        return listings;
    }

}
