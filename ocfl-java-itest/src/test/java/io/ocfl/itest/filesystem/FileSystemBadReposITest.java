package io.ocfl.itest.filesystem;

import io.ocfl.api.MutableOcflRepository;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.cache.NoOpCache;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.itest.BadReposITest;
import io.ocfl.itest.ITestHelper;

public class FileSystemBadReposITest extends BadReposITest {

    protected MutableOcflRepository defaultRepo(String name) {
        var repo = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .inventoryCache(new NoOpCache<>())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .storage(storage ->
                        storage.objectMapper(ITestHelper.prettyPrintMapper()).fileSystem(repoDir(name)))
                .workDir(workDir)
                .buildMutable();
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53Z");
        return repo;
    }
}
