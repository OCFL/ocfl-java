package edu.wisc.library.ocfl.core.itest;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
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

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
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
            repo.getObject(ObjectVersionId.version(objectId, "v3"), outputDir.resolve("out"));
        });

        // ./dot-slash
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v4"), outputDir.resolve("out"));
        });

        // ../dot-dot-slash
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v5"), outputDir.resolve("out"));
        });

        // dir/../dot-dot
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v6"), outputDir.resolve("out"));
        });

        // //empty
        assertThrows(PathConstraintException.class, () -> {
            repo.getObject(ObjectVersionId.version(objectId, "v7"), outputDir.resolve("out"));
        });
    }

    @Test
    public void failWhenSidecarHasInvalidDigest() {
        var repoName = "invalid-sidecar-digest";
        var repoDir = repoDir(repoName);

        assertThat(assertThrows(CorruptObjectException.class, () -> defaultRepo(repoDir)).getMessage(),
                containsString("specifies digest algorithm md5"));
    }

    @Test
    public void failWhenMissingSidecar() {
        var repoName = "missing-sidecar";
        var repoDir = repoDir(repoName);

        assertThat(assertThrows(CorruptObjectException.class, () -> defaultRepo(repoDir)).getMessage(),
                containsString("Expected there to be one inventory sidecar file"));
    }

    @Test
    public void failWhenMissingInventory() {
        var repoName = "missing-inventory";
        var repoDir = repoDir(repoName);

        assertThat(assertThrows(CorruptObjectException.class, () -> defaultRepo(repoDir)).getMessage(),
                containsString("Missing inventory"));
    }

    @Test
    public void failWhenMissingVersion() {
        var repoName = "missing-version";
        var repoDir = repoDir(repoName);
        var repo = defaultRepo(repoDir);

        assertThat(assertThrows(RuntimeIOException.class, () -> repo.getObject(ObjectVersionId.head("o2"), outputDir.resolve("out"))).getMessage(),
                containsString("NoSuchFile"));
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
