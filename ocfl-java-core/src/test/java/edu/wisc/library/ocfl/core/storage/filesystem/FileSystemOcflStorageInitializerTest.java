package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.RepositoryConfigurationException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedNTupleLayoutExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.test.ITestHelper;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.hamcrest.io.FileMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.io.FileMatchers.aFileNamed;
import static org.junit.jupiter.api.Assertions.assertEquals;

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
        var mapper = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldDoNothingWhenAlreadyInitialized() {
        initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());
        var mapper = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldAutodetectConfigWhenAlreadyInitializedAndConfigNotSet() {
        initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());

        var mapper = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                null,
                new ExtensionSupportEvaluator());

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldIgnoreDefaultLayoutWhenRepoHasLayoutConfig() {
        var layoutExt1 = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());
        var layoutExt2 = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig().setTupleSize(1),
                new ExtensionSupportEvaluator());

        assertEquals(layoutExt1.mapObjectId("test"), layoutExt2.mapObjectId("test"));
    }

    @Test
    public void shouldInitWhenAlreadyInitedButHasNoSpecOrObjects() throws IOException {
        initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());
        Files.delete(tempRoot.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(tempRoot, HashedNTupleLayoutExtension.EXTENSION_NAME);

        var mapper = initializer.initializeStorage(tempRoot,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldInitWhenAlreadyInitedHasNoSpecAndHasObjects() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .storage(FileSystemOcflStorage.builder()
                        .repositoryRoot(repoDir)
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .build())
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(repoDir, HashedNTupleLayoutExtension.EXTENSION_NAME);

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        var mapper = initializer.initializeStorage(repoDir,
                OcflVersion.OCFL_1_0,
                new HashedNTupleLayoutConfig(),
                new ExtensionSupportEvaluator());
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldFailWhenAlreadyInitedHasNoSpecAndHasObjectsAndObjectNotFound() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .defaultLayoutConfig(new HashedNTupleLayoutConfig().setTupleSize(2))
                .storage(FileSystemOcflStorage.builder()
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .repositoryRoot(repoDir).build())
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(repoDir, HashedNTupleLayoutExtension.EXTENSION_NAME);

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        OcflAsserts.assertThrowsWithMessage(RepositoryConfigurationException.class, "This layout does not match the layout of existing objects in the repository", () -> {
            initializer.initializeStorage(repoDir,
                    OcflVersion.OCFL_1_0,
                    new HashedNTupleLayoutConfig().setTupleSize(5),
                    new ExtensionSupportEvaluator());
        });
    }

    private void assertRootHasFiles(Path root) {
        var children = children(root);
        assertThat(children, containsInAnyOrder(
                aFileNamed(equalTo("0=ocfl_1.0")),
                aFileNamed(equalTo("ocfl_1.0.txt")),
                aFileNamed(equalTo(OcflConstants.OCFL_LAYOUT)),
                aFileNamed(equalTo(HashedNTupleLayoutExtension.EXTENSION_NAME + ".md")),
                aFileNamed(equalTo(OcflConstants.EXTENSIONS_DIR))));
        assertThat(root.resolve(OcflConstants.EXTENSIONS_DIR)
                .resolve(HashedNTupleLayoutExtension.EXTENSION_NAME)
                .resolve(OcflConstants.EXT_CONFIG_JSON).toFile(), FileMatchers.anExistingFile());
    }

    private List<File> children(Path tempRoot) {
        return Arrays.asList(tempRoot.toFile().listFiles());
    }

    private void deleteExtensionConfig(Path repoRoot, String extensionName) {
        try {
            Files.delete(repoRoot.resolve(OcflConstants.EXTENSIONS_DIR)
                    .resolve(extensionName)
                    .resolve(OcflConstants.EXT_CONFIG_JSON));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
