package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.OcflITest;
import edu.wisc.library.ocfl.test.TestHelper;
import org.junit.jupiter.api.Test;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static edu.wisc.library.ocfl.itest.ITestHelper.*;
import static edu.wisc.library.ocfl.test.TestHelper.inputStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

public class FileSystemOcflITest extends OcflITest {

    private Path reposDir;

    // TODO move once this issue is resolved: https://github.com/adobe/S3Mock/issues/215
    @Test
    public void listObjectsInRepo() {
        var repoName = "repo-list";
        var repo = defaultRepo(repoName, DefaultLayoutConfig.nTupleHashConfig());

        repo.updateObject(ObjectVersionId.head("o1"), defaultCommitInfo, updater -> {
            updater.writeFile(inputStream("test1"), "test1.txt");
        });
        repo.updateObject(ObjectVersionId.head("o2"), defaultCommitInfo, updater -> {
            updater.writeFile(inputStream("test2"), "test2.txt");
        });
        repo.updateObject(ObjectVersionId.head("o3"), defaultCommitInfo, updater -> {
            updater.writeFile(inputStream("test3"), "test3.txt");
        });

        try (var objectIdsStream = repo.listObjectIds()) {
            var objectIds = objectIdsStream.collect(Collectors.toList());
            assertThat(objectIds, containsInAnyOrder("o1", "o2", "o3"));
        }
    }

    @Override
    protected void onBefore() {
        reposDir = UncheckedFiles.createDirectories(tempRoot.resolve("repos"));
    }

    @Override
    protected OcflRepository defaultRepo(String name, LayoutConfig layoutConfig) {
        var repoDir = UncheckedFiles.createDirectories(repoDir(name));
        return existingRepo(name, null, layoutConfig);
    }

    @Override
    protected OcflRepository existingRepo(String name, Path path, LayoutConfig layoutConfig) {
        var repoDir = repoDir(name);

        if (path != null) {
            TestHelper.copyDir(path, repoDir);
        }

        var repo = new OcflRepositoryBuilder()
                .layoutConfig(layoutConfig)
                .inventoryCache(new NoOpCache<>())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .storage(FileSystemOcflStorage.builder()
                        .checkNewVersionFixity(true)
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .repositoryRoot(repoDir)
                        .build())
                .workDir(workDir)
                .build();
        fixTime(repo, "2019-08-05T15:57:53.703314Z");
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
