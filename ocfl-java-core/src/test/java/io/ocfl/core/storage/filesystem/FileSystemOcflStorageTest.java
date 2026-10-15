package io.ocfl.core.storage.filesystem;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.endsWith;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

import io.ocfl.api.DigestAlgorithmRegistry;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.exception.NotFoundException;
import io.ocfl.api.exception.OcflStateException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.extension.ExtensionSupportEvaluator;
import io.ocfl.core.extension.OcflExtensionConfig;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.InventoryBuilder;
import io.ocfl.core.model.Version;
import io.ocfl.core.model.VersionBuilder;
import io.ocfl.core.storage.OcflStorage;
import io.ocfl.core.storage.OcflStorageBuilder;
import io.ocfl.core.test.ITestHelper;
import io.ocfl.core.util.DigestUtil;
import io.ocfl.core.util.FileUtil;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.OffsetDateTime;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class FileSystemOcflStorageTest {

    @TempDir
    public Path tempRoot;

    private Path workDir;
    private Path repoDir;
    private Path stagingDir;
    private Path stagingContentDir;

    private OcflExtensionConfig layoutConfig;

    @BeforeEach
    public void setup() throws IOException {
        workDir = Files.createDirectory(tempRoot.resolve("work"));
        repoDir = Files.createDirectory(tempRoot.resolve("repo"));
        stagingDir = Files.createDirectories(workDir.resolve("staging"));
        stagingContentDir = Files.createDirectories(stagingDir.resolve("content"));

        layoutConfig = new HashedNTupleLayoutConfig();
    }

    @Test
    public void shouldRejectCallsWhenNotInitialized() {
        var storage = OcflStorageBuilder.builder().fileSystem(repoDir).build();

        assertThatThrownBy(() -> {
                    storage.loadInventory("o1");
                })
                .isInstanceOf(OcflStateException.class)
                .hasMessageContaining("must be initialized");
    }

    @Test
    public void shouldListObjectIdsWhenOnlyOne() {
        copyExistingRepo("repo-one-object");
        var storage = newStorage();

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        assertThat(objectIds, containsInAnyOrder("o1"));
    }

    @Test
    public void shouldListObjectIdsWhenMultipleObjects() {
        copyExistingRepo("repo-multiple-objects");
        var storage = newStorage();

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        assertThat(objectIds, containsInAnyOrder("o1", "o2", "o3"));
    }

    @Test
    public void shouldListObjectIdsWhenOnlyNone() {
        copyExistingRepo("repo-no-objects");
        var storage = newStorage();

        var objectIdsStream = storage.listObjectIds();

        var objectIds = objectIdsStream.collect(Collectors.toList());

        Assertions.assertEquals(0, objectIds.size());
    }

    @Test
    public void shouldReturnInventoryBytesWhenExists() {
        copyExistingRepo("repo-multiple-objects");
        var storage = newStorage();

        var bytes = storage.getInventoryBytes("o2", VersionNum.fromInt(2));
        Assertions.assertEquals(
                "c15f51c96fafe599dd056c1782fce5e8d6a0461017260ec5bc751d12821e2a7c2344048fc32312d57fdbdd67"
                        + "ec32e238a5f68e5127a762dd866e77fcddbaa3ce",
                DigestUtil.computeDigestHex(DigestAlgorithmRegistry.sha512, bytes));
    }

    @Test
    public void shouldReturnExceptionWhenInventoryDoesNotExist() {
        copyExistingRepo("repo-multiple-objects");
        var storage = newStorage();

        assertThrows(NotFoundException.class, () -> storage.getInventoryBytes("o2", VersionNum.fromInt(4)));
    }

    @Test
    public void shouldOnlyCreateRetrieverForRequestedFile() throws IOException {
        copyExistingRepo("repo-multiple-objects");
        var fileSystemStorage = spy(new FileSystemStorage(repoDir));
        var storage = newStorage(fileSystemStorage);
        var inventory = storage.loadInventory("o2");

        var streams = storage.getObjectStreams(inventory, VersionNum.fromInt(2));

        assertThat(streams.keySet(), containsInAnyOrder("dir1/dir2/file2", "dir1/file3", "dir3/file1"));
        verify(fileSystemStorage, times(0)).readLazy(anyString(), any(DigestAlgorithm.class), anyString());

        try (var stream = streams.get("dir1/file3").retrieveFile()) {
            Assertions.assertEquals("file 3", new String(stream.readAllBytes(), StandardCharsets.UTF_8));
        }
        verify(fileSystemStorage, times(1)).readLazy(anyString(), any(DigestAlgorithm.class), anyString());
        verify(fileSystemStorage)
                .readLazy(endsWith("v2/content/dir1/file3"), eq(DigestAlgorithmRegistry.sha512), anyString());
    }

    private InventoryBuilder inventoryBuilder() {
        return Inventory.builder()
                .id("o1")
                .type(OcflConstants.DEFAULT_OCFL_VERSION.getInventoryType())
                .digestAlgorithm(DigestAlgorithmRegistry.sha512)
                .objectRootPath(FileUtil.pathToStringStandardSeparator(repoDir.resolve("o1")));
    }

    private VersionBuilder versionBuilder() {
        return Version.builder().created(OffsetDateTime.now());
    }

    private OcflStorage newStorage() {
        return newStorage(new FileSystemStorage(repoDir));
    }

    private OcflStorage newStorage(FileSystemStorage fileSystemStorage) {
        var storage = OcflStorageBuilder.builder().storage(fileSystemStorage).build();
        storage.initializeStorage(
                OcflConstants.DEFAULT_OCFL_VERSION,
                layoutConfig,
                ITestHelper.testInventoryMapper(),
                new ExtensionSupportEvaluator());
        return storage;
    }

    private void copyExistingRepo(String name) {
        copyDir(Paths.get("src/test/resources/repos", name), repoDir);
    }

    private Path copyDir(Path source, Path target) {
        try (var files = Files.walk(source)) {
            files.forEach(f -> {
                try {
                    Files.copy(f, target.resolve(source.relativize(f)), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return target;
    }
}
