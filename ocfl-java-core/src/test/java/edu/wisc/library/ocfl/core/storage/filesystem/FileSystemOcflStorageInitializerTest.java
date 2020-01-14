package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.mapping.FlatObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.NTupleObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.test.ITestHelper;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.*;
import static org.hamcrest.io.FileMatchers.aFileNamed;

public class FileSystemOcflStorageInitializerTest {

    @TempDir
    public Path tempRoot;

    private FileSystemOcflStorageInitializer initializer;

    @BeforeEach
    public void setup() {
        this.initializer = new FileSystemOcflStorageInitializer(ITestHelper.prettyPrintMapper(), new ObjectIdPathMapperBuilder());
    }

    @Test
    public void shouldInitStorageWhenNewWithConfig() {
        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());

        assertRootHasFilesFlat(tempRoot);
        assertThat(mapper, instanceOf(FlatObjectIdPathMapper.class));
    }

    @Test
    public void shouldDoNothingWhenAlreadyInitialized() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());
        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());

        assertRootHasFilesFlat(tempRoot);
        assertThat(mapper, instanceOf(FlatObjectIdPathMapper.class));
    }

    @Test
    public void shouldAutodetectConfigWhenAlreadyInitializedAndConfigNotSet() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.nTupleHashConfig());
        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, null);

        assertRootHasFilesNTuple(tempRoot);
        assertThat(mapper, instanceOf(NTupleObjectIdPathMapper.class));
    }

    @Test
    public void shouldFailWhenConfigOnDiskDoesNotMatch() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.nTupleHashConfig());

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "Storage layout configuration does not match", () -> {
            initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());
        });
    }

    @Test
    public void shouldInitWhenAlreadyInitedButHasNoSpecOrObjects() throws IOException {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.nTupleHashConfig());
        Files.delete(tempRoot.resolve("ocfl_layout.json"));
        Files.delete(tempRoot.resolve("extension-layout-n-tuple.json"));

        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());
        assertThat(mapper, instanceOf(FlatObjectIdPathMapper.class));
    }

    @Test
    public void shouldInitWhenAlreadyInitedHasNoSpecAndHasObjects() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .build(FileSystemOcflStorage.builder()
                        .objectMapper(ITestHelper.prettyPrintMapper())
                .build(repoDir), workDir);

        Files.delete(repoDir.resolve("ocfl_layout.json"));
        Files.delete(repoDir.resolve("extension-layout-flat.json"));

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        var mapper = initializer.initializeStorage(repoDir, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());
        assertThat(mapper, instanceOf(FlatObjectIdPathMapper.class));
    }

    @Test
    public void shouldFailWhenAlreadyInitedHasNoSpecAndHasObjectsAndObjectNotFound() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .build(FileSystemOcflStorage.builder()
                        .objectMapper(ITestHelper.prettyPrintMapper())
                .build(repoDir), workDir);

        Files.delete(repoDir.resolve("ocfl_layout.json"));
        Files.delete(repoDir.resolve("extension-layout-flat.json"));

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "This layout does not match the layout of existing objects in the repository", () -> {
            initializer.initializeStorage(repoDir, OcflVersion.OCFL_1_0, DefaultLayoutConfig.nTupleHashConfig());
        });
    }

    @Test
    public void shouldFailWhenHasSpecButNoConfig() throws IOException {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.nTupleHashConfig());
        Files.delete(tempRoot.resolve("extension-layout-n-tuple.json"));

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "Missing layout extension configuration", () -> {
            initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, DefaultLayoutConfig.flatUrlConfig());
        });
    }

    private void assertRootHasFilesFlat(Path root) {
        var children = children(root);
        assertThat(children, containsInAnyOrder(
                aFileNamed(equalTo("0=ocfl_1.0")),
                aFileNamed(equalTo("ocfl_1.0.txt")),
                aFileNamed(equalTo("ocfl_layout.json")),
                aFileNamed(equalTo("extension-layout-flat.json"))));
    }

    private void assertRootHasFilesNTuple(Path root) {
        var children = children(root);
        assertThat(children, containsInAnyOrder(
                aFileNamed(equalTo("0=ocfl_1.0")),
                aFileNamed(equalTo("ocfl_1.0.txt")),
                aFileNamed(equalTo("ocfl_layout.json")),
                aFileNamed(equalTo("extension-layout-n-tuple.json"))));
    }

    private List<File> children(Path tempRoot) {
        return Arrays.asList(tempRoot.toFile().listFiles());
    }

}
