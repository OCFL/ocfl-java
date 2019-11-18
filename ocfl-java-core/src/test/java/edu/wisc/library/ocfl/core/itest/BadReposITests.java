package edu.wisc.library.ocfl.core.itest;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BadReposITests {

    @TempDir
    public Path tempRoot;

    private Path workDir;
    private Path outputDir;

    @BeforeEach
    public void setup() throws IOException {
        workDir = Files.createDirectory(tempRoot.resolve("work"));
        outputDir = Files.createDirectory(tempRoot.resolve("output"));
    }

    @Test
    public void shouldFailGetObjectWhenLogicalPathContainsIllegalChars() {
        var repoName = "bad-logical-paths";
        var repoDir = repoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        // /leading-slash
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v3"), outputDir);
        });

        // ./dot-slash
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v4"), outputDir);
        });

        // ../dot-dot-slash
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v5"), outputDir);
        });

        // dir/../dot-dot
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v6"), outputDir);
        });

        // //empty
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v7"), outputDir);
        });
    }

    private Path repoDir(String name) {
        return Paths.get("src/test/resources/invalid-repos", name);
    }

    private MutableOcflRepository defaultRepo(Path repoDir) {
        var repo = new OcflRepositoryBuilder().inventoryMapper(ITestHelper.testInventoryMapper()).buildMutable(
                new FileSystemOcflStorageBuilder()
                        .checkNewVersionFixity(true)
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .build(repoDir, new ObjectIdPathMapperBuilder().buildFlatMapper()),
                workDir);
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

}
