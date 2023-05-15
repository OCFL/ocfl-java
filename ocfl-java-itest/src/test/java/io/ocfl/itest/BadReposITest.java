package io.ocfl.itest;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.exception.CorruptObjectException;
import io.ocfl.api.exception.PathConstraintException;
import io.ocfl.api.model.ObjectVersionId;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class BadReposITest {

    @TempDir
    public Path tempRoot;

    protected Path workDir;
    protected Path outputDir;

    @BeforeEach
    public void setup() throws IOException {
        workDir = Files.createDirectory(tempRoot.resolve("work"));
        outputDir = Files.createDirectory(tempRoot.resolve("output"));

        onBefore();
    }

    @AfterEach
    public void after() {
        onAfter();
    }

    protected void onBefore() {}

    protected void onAfter() {}

    protected abstract MutableOcflRepository defaultRepo(String name);

    @Test
    public void shouldFailGetObjectWhenLogicalPathContainsIllegalChars() {
        var repoName = "bad-logical-paths";
        var repo = defaultRepo(repoName);

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
        var repo = defaultRepo(repoName);

        assertThat(
                assertThrows(CorruptObjectException.class, () -> repo.describeObject("o2"))
                        .getMessage(),
                containsString("sidecar"));
    }

    @Test
    public void failWhenMissingSidecar() {
        var repoName = "missing-sidecar";
        var repo = defaultRepo(repoName);
        assertThat(
                assertThrows(CorruptObjectException.class, () -> repo.describeObject("o2"))
                        .getMessage(),
                containsString("sidecar"));
    }

    @Test
    public void failWhenMissingInventory() {
        var repoName = "missing-inventory";

        assertThat(
                assertThrows(CorruptObjectException.class, () -> defaultRepo(repoName))
                        .getMessage(),
                containsString("Missing inventory"));
    }

    @Test
    public void failWhenMissingVersion() {
        var repoName = "missing-version";
        var repo = defaultRepo(repoName);

        assertThat(
                assertThrows(
                                RuntimeException.class,
                                () -> repo.getObject(ObjectVersionId.head("o2"), outputDir.resolve("out")))
                        .getMessage(),
                either(containsString("NoSuchFile")).or(containsString("not found")));
    }

    protected Path repoDir(String name) {
        return Paths.get("src/test/resources/invalid-repos", name);
    }
}
