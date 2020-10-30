package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.exception.RepositoryConfigurationException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleExtension;
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
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.io.FileMatchers.aFileNamed;

public class FileSystemOcflStorageInitializerTest {

    @TempDir
    public Path tempRoot;

    private FileSystemOcflStorageInitializer initializer;

    @BeforeEach
    public void setup() {
        this.initializer = new FileSystemOcflStorageInitializer(ITestHelper.prettyPrintMapper());
    }

    @Test
    public void shouldInitStorageWhenNewWithConfig() {
        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedTruncatedNTupleExtension.class));
    }

    @Test
    public void shouldDoNothingWhenAlreadyInitialized() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());
        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedTruncatedNTupleExtension.class));
    }

    @Test
    public void shouldAutodetectConfigWhenAlreadyInitializedAndConfigNotSet() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());

        var mapper = initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, null);

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedTruncatedNTupleExtension.class));
    }

    @Test
    public void shouldFailWhenConfigOnDiskDoesNotMatch() {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());

        OcflAsserts.assertThrowsWithMessage(RepositoryConfigurationException.class, "Storage layout configuration does not match", () -> {
            initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig().setTupleSize(1));
        });
    }

    @Test
    public void shouldInitWhenAlreadyInitedButHasNoSpecOrObjects() throws IOException {
        initializer.initializeStorage(tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());
        Files.delete(tempRoot.resolve("ocfl_layout.json"));
        Files.delete(tempRoot.resolve(HashedTruncatedNTupleExtension.EXTENSION_NAME + ".json"));

        var mapper = initializer.initializeStorage(
                tempRoot, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());
        assertThat(mapper, instanceOf(HashedTruncatedNTupleExtension.class));
    }

    @Test
    public void shouldInitWhenAlreadyInitedHasNoSpecAndHasObjects() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .storage(FileSystemOcflStorage.builder()
                        .repositoryRoot(repoDir)
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .build())
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve("ocfl_layout.json"));
        Files.delete(repoDir.resolve(HashedTruncatedNTupleExtension.EXTENSION_NAME + ".json"));

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        var mapper = initializer.initializeStorage(
                repoDir, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig());
        assertThat(mapper, instanceOf(HashedTruncatedNTupleExtension.class));
    }

    @Test
    public void shouldFailWhenAlreadyInitedHasNoSpecAndHasObjectsAndObjectNotFound() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .layoutConfig(new HashedTruncatedNTupleConfig().setTupleSize(2))
                .storage(FileSystemOcflStorage.builder()
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .repositoryRoot(repoDir).build())
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve("ocfl_layout.json"));
        Files.delete(repoDir.resolve(HashedTruncatedNTupleExtension.EXTENSION_NAME + ".json"));

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        OcflAsserts.assertThrowsWithMessage(RepositoryConfigurationException.class, "This layout does not match the layout of existing objects in the repository", () -> {
            initializer.initializeStorage(repoDir, OcflVersion.OCFL_1_0, new HashedTruncatedNTupleConfig().setTupleSize(5));
        });
    }

    private void assertRootHasFiles(Path root) {
        var children = children(root);
        assertThat(children, containsInAnyOrder(
                aFileNamed(equalTo("0=ocfl_1.0")),
                aFileNamed(equalTo("ocfl_1.0.txt")),
                aFileNamed(equalTo("ocfl_layout.json")),
                aFileNamed(equalTo(HashedTruncatedNTupleExtension.EXTENSION_NAME + ".json"))));
    }

    private List<File> children(Path tempRoot) {
        return Arrays.asList(tempRoot.toFile().listFiles());
    }

}
