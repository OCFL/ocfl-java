package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedNTupleLayoutExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatLayoutConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.OcflITest;
import edu.wisc.library.ocfl.test.TestHelper;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static edu.wisc.library.ocfl.itest.ITestHelper.expectedRepoPath;
import static edu.wisc.library.ocfl.itest.ITestHelper.fixTime;
import static edu.wisc.library.ocfl.itest.ITestHelper.sourceObjectPath;
import static edu.wisc.library.ocfl.itest.ITestHelper.streamString;
import static edu.wisc.library.ocfl.itest.ITestHelper.verifyDirectoryContentsSame;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class FileSystemOcflITest extends OcflITest {

    private Path reposDir;

    // Does not work with S3Mock because the generated filenames are too long
    @Test
    public void hashedIdLayout() {
        var repoName = "hashed-id-layout";
        var repo = defaultRepo(repoName, builder -> builder.defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig()));

        var objectIds = List.of(
                "o1",
                "http://library.wisc.edu/123",
                "abcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghijabcdefghija");

        objectIds.forEach(objectId -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
                updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                        .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
            });
        });

        verifyRepo(repoName);
    }

    // This test doesn't work with S3Mock because it double encodes
    @Test
    public void hashedIdLayoutLongEncoded() {
        var repoName = "hashed-id-layout-2";
        var repo = defaultRepo(repoName, builder -> builder.defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig()));

        var objectId = "۵ݨݯژښڙڜڛڝڠڱݰݣݫۯ۞ۆݰ";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        verifyRepo(repoName);
    }

    @Test
    public void purgeShouldRemoveEmptyParentDirs() throws IOException {
        var repoName = "purge-empty-dirs";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.purgeObject(objectId);

        assertThat(new ArrayList<>(Arrays.asList(repoDir(repoName).toFile().list())),
                containsInAnyOrder("0=ocfl_1.0", "ocfl_1.0.txt", "ocfl_extensions_1.0.md",
                        OcflConstants.EXTENSIONS_DIR, OcflConstants.OCFL_LAYOUT,
                        HashedNTupleLayoutExtension.EXTENSION_NAME + ".md"));
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void allowPathsWithDifficultCharsWhenNoRestrictionsApplied() throws IOException {
        var repoName = "repo16";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("test1".getBytes()), "backslash\\path\\file");
            updater.writeFile(new ByteArrayInputStream("test3".getBytes()), "fi\u0080le");
        });

        var expectedRepoPath = expectedRepoPath(repoName);
        var backslashFile = expectedRepoPath.resolve(O1_PATH + "/v1/content/backslash\\path\\file");
        try {
            Files.write(backslashFile, "test1".getBytes());
            verifyRepo(repoName);
        } finally {
            Files.deleteIfExists(backslashFile);
        }
    }

    @Test
    public void shouldNotCreateEmptyContentDirWhenVersionHasNoContent() {
        var repoName = "empty-content";
        var repo = defaultRepo(repoName, builder -> builder.defaultLayoutConfig(new FlatLayoutConfig()));

        var objectId = "object";

        repo.updateObject(ObjectVersionId.head(objectId), null, updater -> {
            updater.writeFile(streamString("asdf"), "file.txt");
        });

        repo.updateObject(ObjectVersionId.head(objectId), null, updater -> {
            updater.removeFile("file.txt");
        });

        var root = repoDir(repoName);
        var v2ContentPath = root.resolve(objectId).resolve("v2/content");

        assertFalse(Files.exists(v2ContentPath), "empty content directories should not exist");
    }

    @Override
    protected void onBefore() {
        reposDir = UncheckedFiles.createDirectories(tempRoot.resolve("repos"));
    }

    @Override
    protected OcflRepository defaultRepo(String name, Consumer<OcflRepositoryBuilder> consumer) {
        UncheckedFiles.createDirectories(repoDir(name));
        return existingRepo(name, null, consumer);
    }

    @Override
    protected OcflRepository existingRepo(String name, Path path, Consumer<OcflRepositoryBuilder> consumer) {
        var repoDir = repoDir(name);

        if (path != null) {
            TestHelper.copyDir(path, repoDir);
        }

        var builder = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .inventoryCache(new NoOpCache<>())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .storage(storage -> storage
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .fileSystem(repoDir)
                        .build())
                .workDir(workDir);

        consumer.accept(builder);

        var repo = builder.build();
        fixTime(repo, "2019-08-05T15:57:53Z");
        return repo;
    }

    @Override
    protected void verifyRepo(String name) {
        verifyDirectoryContentsSame(expectedRepoPath(name), repoDir(name));
    }

    @Override
    protected List<String> listFilesInRepo(String name) {
        return ITestHelper.listAllPaths(repoDir(name)).stream()
                .map(FileUtil::pathToStringStandardSeparator).collect(Collectors.toList());
    }

    private Path repoDir(String name) {
        return reposDir.resolve(name);
    }

}
