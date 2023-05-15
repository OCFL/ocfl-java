package io.ocfl.core.storage.filesystem;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.io.FileMatchers.aFileNamed;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.ocfl.api.OcflConstants;
import io.ocfl.api.exception.RepositoryConfigurationException;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.OcflVersion;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.ExtensionSupportEvaluator;
import io.ocfl.core.extension.storage.layout.HashedNTupleLayoutExtension;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.storage.DefaultOcflStorageInitializer;
import io.ocfl.core.test.ITestHelper;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import org.hamcrest.io.FileMatchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileSystemOcflStorageInitializerTest {

    @TempDir
    public Path tempRoot;

    private DefaultOcflStorageInitializer initializer;

    @BeforeEach
    public void setup() {
        this.initializer =
                new DefaultOcflStorageInitializer(new FileSystemStorage(tempRoot), ITestHelper.prettyPrintMapper());
    }

    @Test
    public void shouldInitStorageWhenNewWithConfig() {
        var mapper = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldDoNothingWhenAlreadyInitialized() {
        initializer.initializeStorage(
                OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator());
        var mapper = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldAutodetectConfigWhenAlreadyInitializedAndConfigNotSet() {
        initializer.initializeStorage(
                OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator());

        var mapper = initializer
                .initializeStorage(OcflVersion.OCFL_1_0, null, new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();

        assertRootHasFiles(tempRoot);
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldIgnoreDefaultLayoutWhenRepoHasLayoutConfig() {
        var layoutExt1 = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();
        var layoutExt2 = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0,
                        new HashedNTupleLayoutConfig().setTupleSize(1),
                        new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();

        assertEquals(layoutExt1.mapObjectId("test"), layoutExt2.mapObjectId("test"));
    }

    @Test
    public void shouldInitWhenAlreadyInitedButHasNoSpecOrObjects() throws IOException {
        initializer.initializeStorage(
                OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator());
        Files.delete(tempRoot.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(tempRoot, HashedNTupleLayoutExtension.EXTENSION_NAME);

        var mapper = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldInitWhenAlreadyInitedHasNoSpecAndHasObjects() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .storage(storage -> {
                    storage.objectMapper(ITestHelper.prettyPrintMapper()).fileSystem(repoDir);
                })
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(repoDir, HashedNTupleLayoutExtension.EXTENSION_NAME);

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        this.initializer =
                new DefaultOcflStorageInitializer(new FileSystemStorage(repoDir), ITestHelper.prettyPrintMapper());
        var mapper = initializer
                .initializeStorage(
                        OcflVersion.OCFL_1_0, new HashedNTupleLayoutConfig(), new ExtensionSupportEvaluator())
                .getStorageLayoutExtension();
        assertThat(mapper, instanceOf(HashedNTupleLayoutExtension.class));
    }

    @Test
    public void shouldFailWhenAlreadyInitedHasNoSpecAndHasObjectsAndObjectNotFound() throws IOException {
        var repoDir = Files.createDirectories(tempRoot.resolve("repo"));
        var workDir = Files.createDirectories(tempRoot.resolve("work"));
        var repo = new OcflRepositoryBuilder()
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .defaultLayoutConfig(new HashedNTupleLayoutConfig().setTupleSize(2))
                .storage(storage -> {
                    storage.objectMapper(ITestHelper.prettyPrintMapper()).fileSystem(repoDir);
                })
                .workDir(workDir)
                .build();

        Files.delete(repoDir.resolve(OcflConstants.OCFL_LAYOUT));
        deleteExtensionConfig(repoDir, HashedNTupleLayoutExtension.EXTENSION_NAME);

        repo.updateObject(ObjectVersionId.head("blah/blah"), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("blah".getBytes()), "file1");
        });

        this.initializer =
                new DefaultOcflStorageInitializer(new FileSystemStorage(repoDir), ITestHelper.prettyPrintMapper());

        assertThatThrownBy(() -> {
                    initializer.initializeStorage(
                            OcflVersion.OCFL_1_0,
                            new HashedNTupleLayoutConfig().setTupleSize(5),
                            new ExtensionSupportEvaluator());
                })
                .isInstanceOf(RepositoryConfigurationException.class)
                .hasMessageContaining("This layout does not match the layout of existing objects in the repository");
    }

    private void assertRootHasFiles(Path root) {
        var children = children(root);
        assertThat(
                children,
                containsInAnyOrder(
                        aFileNamed(equalTo("0=ocfl_1.0")),
                        aFileNamed(equalTo("ocfl_1.0.txt")),
                        aFileNamed(equalTo("ocfl_extensions_1.0.md")),
                        aFileNamed(equalTo(OcflConstants.OCFL_LAYOUT)),
                        aFileNamed(equalTo(HashedNTupleLayoutExtension.EXTENSION_NAME + ".md")),
                        aFileNamed(equalTo(OcflConstants.EXTENSIONS_DIR))));
        assertThat(
                root.resolve(OcflConstants.EXTENSIONS_DIR)
                        .resolve(HashedNTupleLayoutExtension.EXTENSION_NAME)
                        .resolve(OcflConstants.EXT_CONFIG_JSON)
                        .toFile(),
                FileMatchers.anExistingFile());
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
