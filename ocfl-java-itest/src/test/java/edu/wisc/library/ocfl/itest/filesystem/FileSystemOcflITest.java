package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorageBuilder;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.OcflITest;
import edu.wisc.library.ocfl.test.TestHelper;

import java.nio.file.Path;
import java.util.List;
import java.util.stream.Collectors;

import static edu.wisc.library.ocfl.itest.ITestHelper.*;

public class FileSystemOcflITest extends OcflITest {

    private Path reposDir;

    @Override
    protected void onBefore() {
        reposDir = SafeFiles.createDirectories(tempRoot.resolve("repos"));
    }

    @Override
    protected OcflRepository defaultRepo(String name) {
        var repoDir = SafeFiles.createDirectories(repoDir(name));
        return existingRepo(name, null);
    }

    @Override
    protected OcflRepository existingRepo(String name, Path path) {
        var repoDir = repoDir(name);

        if (path != null) {
            TestHelper.copyDir(path, repoDir);
        }

        var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .inventoryCache(new NoOpCache<>())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .storage(new FileSystemOcflStorageBuilder()
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
