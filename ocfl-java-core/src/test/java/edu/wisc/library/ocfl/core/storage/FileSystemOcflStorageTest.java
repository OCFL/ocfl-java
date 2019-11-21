package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.model.*;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileSystemOcflStorageTest {

    @TempDir
    public Path tempRoot;

    private Path workDir;
    private Path repoDir;
    private Path stagingDir;
    private Path stagingContentDir;

    private ObjectIdPathMapper objectIdPathMapper;

    @BeforeEach
    public void setup() throws IOException {
        workDir = Files.createDirectory(tempRoot.resolve("work"));
        repoDir = Files.createDirectory(tempRoot.resolve("repo"));
        stagingDir = Files.createDirectories(workDir.resolve("staging"));
        stagingContentDir = Files.createDirectories(stagingDir.resolve("content"));

        objectIdPathMapper = new ObjectIdPathMapperBuilder().buildFlatMapper();
    }

    @Test
    public void shouldFailNewVersionMissingFilesNoFixityCheck() throws IOException {
        var storage = newStorage(false);

        Files.writeString(stagingContentDir.resolve("file1"), "file1 content");

        var inventory = inventoryBuilder()
                .addFileToManifest("1", "v1/content/file1")
                .addFileToManifest("2", "v1/content/file2")
                .addHeadVersion(versionBuilder()
                        .addFile("1", "file1")
                        .addFile("2", "file2")
                        .build())
                .build();

        assertThat(assertThrows(CorruptObjectException.class, () -> storage.storeNewVersion(inventory, stagingDir)).getMessage(),
                containsString("Object o1 is missing the following files: [v1/content/file2]"));
    }

    @Test
    public void shouldFailNewVersionHasExtraFilesNoFixityCheck() throws IOException {
        var storage = newStorage(false);

        Files.writeString(stagingContentDir.resolve("file1"), "file1 content");
        Files.writeString(stagingContentDir.resolve("file2"), "file2 content");
        Files.writeString(stagingContentDir.resolve("file3"), "file3 content");

        var inventory = inventoryBuilder()
                .addFileToManifest("1", "v1/content/file1")
                .addFileToManifest("2", "v1/content/file2")
                .addHeadVersion(versionBuilder()
                        .addFile("1", "file1")
                        .addFile("2", "file2")
                        .build())
                .build();

        assertThat(assertThrows(CorruptObjectException.class, () -> storage.storeNewVersion(inventory, stagingDir)).getMessage(),
                containsString("File not listed in object o1 manifest: v1/content/file3"));
    }

    @Test
    public void shouldFailNewVersionWhenFixityCheckEnabledAndFixityFails() throws IOException {
        var storage = newStorage(true);

        Files.writeString(stagingContentDir.resolve("file1"), "file1 content");

        var inventory = inventoryBuilder()
                .addFileToManifest("1", "v1/content/file1")
                .addHeadVersion(versionBuilder()
                        .addFile("1", "file1")
                        .build())
                .build();

        assertThat(assertThrows(FixityCheckException.class, () -> storage.storeNewVersion(inventory, stagingDir)).getMessage(),
                containsString("file1 in object o1 failed its sha512 fixity check. Expected: 1; Actual: "));
    }

    private InventoryBuilder inventoryBuilder() {
        return Inventory.builder()
                .id("o1")
                .type(OcflConstants.DEFAULT_INVENTORY_TYPE)
                .digestAlgorithm(DigestAlgorithm.sha512)
                .objectRootPath(FileUtil.pathToStringStandardSeparator(repoDir.resolve("o1")));
    }

    private VersionBuilder versionBuilder() {
        return Version.builder().created(OffsetDateTime.now());
    }

    private FileSystemOcflStorage newStorage(boolean enableFixityCheck) {
        var storage = FileSystemOcflStorage.builder()
                .checkNewVersionFixity(enableFixityCheck)
                .build(repoDir, objectIdPathMapper);
        storage.initializeStorage(OcflConstants.OCFL_VERSION);
        return storage;
    }

}
