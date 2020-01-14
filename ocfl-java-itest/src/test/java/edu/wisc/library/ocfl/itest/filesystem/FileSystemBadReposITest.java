package edu.wisc.library.ocfl.itest.filesystem;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.cache.NoOpCache;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorageBuilder;
import edu.wisc.library.ocfl.itest.BadReposITest;
import edu.wisc.library.ocfl.itest.ITestHelper;

public class FileSystemBadReposITest extends BadReposITest {

    protected MutableOcflRepository defaultRepo(String name) {
        var repo = new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .inventoryCache(new NoOpCache<>())
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .buildMutable(new FileSystemOcflStorageBuilder()
                                .checkNewVersionFixity(true)
                                .objectMapper(ITestHelper.prettyPrintMapper())
                                .build(repoDir(name)),
                        workDir);
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

}
