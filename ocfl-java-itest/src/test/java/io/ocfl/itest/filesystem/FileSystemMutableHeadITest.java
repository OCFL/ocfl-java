package io.ocfl.itest.filesystem;

import static io.ocfl.itest.ITestHelper.verifyDirectoryContentsSame;

import io.ocfl.api.MutableOcflRepository;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.cache.NoOpCache;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.util.UncheckedFiles;
import io.ocfl.itest.ITestHelper;
import io.ocfl.itest.MutableHeadITest;
import io.ocfl.itest.TestHelper;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Consumer;

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
                .storage(storage ->
                        storage.objectMapper(ITestHelper.prettyPrintMapper()).fileSystem(repoDir))
                .workDir(workDir);

        consumer.accept(builder);

        var repo = builder.buildMutable();
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53Z");
        return repo;
    }

    @Override
    protected void verifyRepo(String name) {
        ITestHelper.verifyDirectoryContentsSame(ITestHelper.expectedRepoPath(name), repoDir(name));
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
