package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryBuilder;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.model.VersionBuilder;
import edu.wisc.library.ocfl.core.test.ITestHelper;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FileSystemOcflStorageTest {

    @TempDir
    public Path tempRoot;

    private Path workDir;
    private Path repoDir;
    private Path stagingDir;
    private Path stagingContentDir;

    private LayoutConfig layoutConfig;

    @BeforeEach
    public void setup() throws IOException {
        workDir = Files.createDirectory(tempRoot.resolve("work"));
        repoDir = Files.createDirectory(tempRoot.resolve("repo"));
        stagingDir = Files.createDirectories(workDir.resolve("staging"));
        stagingContentDir = Files.createDirectories(stagingDir.resolve("content"));

        layoutConfig = DefaultLayoutConfig.flatUrlConfig();
    }

    @Test
    public void shouldRejectCallsWhenNotInitialized() {
        var storage = FileSystemOcflStorage.builder()
                .checkNewVersionFixity(true)
                .repositoryRoot(repoDir)
                .build();

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "must be initialized", () -> {
            storage.loadInventory("o1");
        });
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

    @Test
    public void shouldListObjectIdsWhenOnlyOne() {
        var storage = FileSystemOcflStorage.builder()
                .repositoryRoot(existingRepoRoot("repo-one-object"))
                .build();
        storage.initializeStorage(OcflConstants.DEFAULT_OCFL_VERSION, DefaultLayoutConfig.nTupleHashConfig(), ITestHelper.testInventoryMapper());

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        assertThat(objectIds, containsInAnyOrder("o1"));
    }

    @Test
    public void shouldListObjectIdsWhenMultipleObjects() {
        var storage = FileSystemOcflStorage.builder()
                .repositoryRoot(existingRepoRoot("repo-multiple-objects"))
                .build();
        storage.initializeStorage(OcflConstants.DEFAULT_OCFL_VERSION, DefaultLayoutConfig.nTupleHashConfig(), ITestHelper.testInventoryMapper());

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        assertThat(objectIds, containsInAnyOrder("o1", "o2", "o3"));
    }

    @Test
    public void shouldListObjectIdsWhenOnlyNone() {
        var storage = FileSystemOcflStorage.builder()
                .repositoryRoot(existingRepoRoot("repo-no-objects"))
                .build();
        storage.initializeStorage(OcflConstants.DEFAULT_OCFL_VERSION, DefaultLayoutConfig.nTupleHashConfig(), ITestHelper.testInventoryMapper());

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        assertEquals(0, objectIds.size());
    }

    private InventoryBuilder inventoryBuilder() {
        return Inventory.builder()
                .id("o1")
                .type(OcflConstants.DEFAULT_OCFL_VERSION.getInventoryType())
                .digestAlgorithm(DigestAlgorithm.sha512)
                .objectRootPath(FileUtil.pathToStringStandardSeparator(repoDir.resolve("o1")));
    }

    private VersionBuilder versionBuilder() {
        return Version.builder().created(OffsetDateTime.now());
    }

    private FileSystemOcflStorage newStorage(boolean enableFixityCheck) {
        var storage = FileSystemOcflStorage.builder()
                .checkNewVersionFixity(enableFixityCheck)
                .repositoryRoot(repoDir)
                .build();
        storage.initializeStorage(OcflConstants.DEFAULT_OCFL_VERSION, layoutConfig, ITestHelper.testInventoryMapper());
        return storage;
    }

    private Path existingRepoRoot(String name) {
        return Paths.get("src/test/resources/repos/" + name);
    }

}
