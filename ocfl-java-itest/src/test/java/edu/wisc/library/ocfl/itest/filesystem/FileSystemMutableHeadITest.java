package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.itest.ITestHelper;
import edu.wisc.library.ocfl.itest.MutableHeadITest;
import edu.wisc.library.ocfl.test.TestHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

import static edu.wisc.library.ocfl.itest.ITestHelper.expectedRepoPath;
import static edu.wisc.library.ocfl.itest.ITestHelper.fixTime;
import static edu.wisc.library.ocfl.itest.ITestHelper.verifyDirectoryContentsSame;

public class FileSystemMutableHeadITest extends MutableHeadITest {

    @Override
    protected MutableOcflRepository defaultRepo(String name, Consumer<OcflRepositoryBuilder> consumer) {
        UncheckedFiles.createDirectories(repoDir(name));
        return existingRepo(name, null, consumer);
    }

    @Override
    protected MutableOcflRepository existingRepo(String name, Path path, Consumer<OcflRepositoryBuilder> consumer) {
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

        var repo = builder.buildMutable();
        fixTime(repo, "2019-08-05T15:57:53Z");
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
