package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorageBuilder;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.MutableHeadITest;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

import static edu.wisc.library.ocfl.itest.ITestHelper.expectedRepoPath;
import static edu.wisc.library.ocfl.itest.ITestHelper.verifyDirectoryContentsSame;

public class FileSystemMutableHeadITest extends MutableHeadITest {

    @Override
    protected MutableOcflRepository defaultRepo(String name) {
        var repoDir = SafeFiles.createDirectories(repoDir(name));
        var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .buildMutable(new FileSystemOcflStorageBuilder()
                                .checkNewVersionFixity(true)
                                .objectMapper(ITestHelper.prettyPrintMapper())
                                .build(repoDir),
                        workDir);
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

    @Override
    protected void verifyRepo(String name) {
        verifyDirectoryContentsSame(expectedRepoPath(name), repoDir(name));
    }

    @Override
    protected void writeFile(String repoName, String path, InputStream content) {
        try {
            Files.write(repoDir(repoName).resolve(path), content.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path repoDir(String name) {
        return reposDir.resolve(name);
    }

}
