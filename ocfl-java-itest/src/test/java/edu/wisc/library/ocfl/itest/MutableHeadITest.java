package edu.wisc.library.ocfl.itest;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.test.OcflAsserts;
import edu.wisc.library.ocfl.test.TestHelper;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Consumer;

import static edu.wisc.library.ocfl.itest.ITestHelper.expectedRepoPath;
import static edu.wisc.library.ocfl.itest.ITestHelper.sourceObjectPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class MutableHeadITest {

    private static final String O1_PATH = "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc";

    @TempDir
    public Path tempRoot;

    protected Path outputDir;
    protected Path reposDir;
    protected Path workDir;

    protected VersionInfo defaultVersionInfo;

    @BeforeEach
    public void setup() throws IOException {
        outputDir = Files.createDirectory(tempRoot.resolve("output"));
        reposDir = Files.createDirectory(tempRoot.resolve("repos"));
        workDir = Files.createDirectory(tempRoot.resolve("work"));

        defaultVersionInfo =  new VersionInfo().setMessage("commit message")
                .setUser("Peter", "peter@example.com");

        onBefore();
    }

    @AfterEach
    public void after() {
        onAfter();
    }

    protected MutableOcflRepository defaultRepo(String name) {
        return defaultRepo(name, builder -> builder.layoutConfig(new HashedTruncatedNTupleConfig()));
    }

    protected abstract MutableOcflRepository defaultRepo(String name, Consumer<OcflRepositoryBuilder> consumer);

    protected MutableOcflRepository existingRepo(String name, Path path) {
        return existingRepo(name, path, builder -> builder.layoutConfig(new HashedTruncatedNTupleConfig()));
    }

    protected abstract MutableOcflRepository existingRepo(String name, Path path, Consumer<OcflRepositoryBuilder> consumer);

    protected abstract void verifyRepo(String name);

    protected abstract void writeFile(String repoName, String path, InputStream content);

    protected void onBefore() {

    }

    protected void onAfter() {

    }

    @Test
    public void createMutableHeadWhenObjectExistsAndDoesNotHaveMutableHead() {
        var repoName = "mutable1";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5" .getBytes()), "file5")
                    .renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);

        var details = repo.describeObject(objectId);

        assertTrue(details.getHeadVersion().isMutable(), "HEAD isMutable");
        assertFalse(details.getVersion(VersionId.fromString("v1")).isMutable(), "v1 isMutable");
    }

    @Test
    public void shouldNotIncludeEmptyRevisionDirectoriesInMutableHeadContent() {
        var repoName = "mutable6";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);

        var details = repo.describeObject(objectId);

        assertTrue(details.getHeadVersion().isMutable(), "HEAD isMutable");
        assertFalse(details.getVersion(VersionId.fromString("v1")).isMutable(), "v1 isMutable");
    }

    @Test
    public void purgeMutableHeadWhenExists() {
        var repoName = "mutable4";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5" .getBytes()), "file5")
                    .renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        repo.purgeStagedChanges(objectId);

        assertFalse(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);
    }

    @Test
    public void createMutableHeadOnNewObject() {
        var repoName = "mutable2";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4")
                    .removeFile("dir1/file3");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);
    }

    @Test
    public void commitStagedVersionWhenHadMutableHeadAndValid() {
        var repoName = "mutable3";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4").removeFile("dir1/file3");
        });

        repo.commitStagedChanges(objectId, defaultVersionInfo.setMessage("commit"));

        assertFalse(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);
    }

    @Test
    public void failWhenCreatingNewVersionOnObjectWithMutableHead() {
        var repoName = "mutable5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });

        assertThat(assertThrows(IllegalStateException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("update"), updater -> {
                updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4");
            });
        }).getMessage(), Matchers.containsString("it has an active mutable HEAD"));
    }

    @Test
    public void shouldFailWhenRevisionMarkerAlreadyExists() {
        var repoName = "mutable5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });

        OcflAsserts.assertThrowsWithMessage(ObjectOutOfSyncException.class, "Changes are out of sync with the current object state", () -> {
            repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
                writeFile(repoName, O1_PATH + "/extensions/0004-mutable-head/revisions/r2", TestHelper.inputStream("r2"));
                updater.writeFile(new ByteArrayInputStream("file5" .getBytes()), "file5")
                        .renameFile("dir1/file3", "file3")
                        .removeFile("dir1/file4");
            });
        });

        verifyRepo(repoName);
    }

    @Test
    public void shouldFailReplicateWhenMutableHeadExists() {
        var repoName = "mutable6";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3")
                    .removeFile("f1");
        });

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "has an active mutable HEAD", () -> {
            repo.replicateVersionAsHead(ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("replicate"));
        });
    }

    @Test
    public void shouldRollbackWhenMutableHeadExists() {
        var repoName = "rollback1";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3")
                    .removeFile("f1");
        });

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });

        repo.rollbackToVersion(ObjectVersionId.version(objectId, "v1"));

        verifyRepo(repoName);
    }

    @Test
    public void hasStagedChangesShouldReturnFalseWhenObjectDoesNotExist() {
        var repoName = "mutable7";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        assertFalse(repo.hasStagedChanges(objectId));
    }

    @Test
    public void failExportObjectVersionWhenVersionIsMutableHead() {
        var repoName = "mutable5";
        var repoRoot = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoRoot);

        var output = outputPath(repoName, "o1-head");

        OcflAsserts.assertThrowsWithMessage(NotFoundException.class, "Object o1 version v2", () -> {
            repo.exportVersion(ObjectVersionId.version("o1", "v2"), output);
        });
    }

    @Test
    public void exportObjectVersionWhenObjectHasMutableHead() {
        var repoName = "mutable5";
        var repoRoot = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoRoot);

        var output = outputPath(repoName, "o1v1");

        repo.exportVersion(ObjectVersionId.version("o1", "v1"), output);

        ITestHelper.verifyDirectoryContentsSame(
                repoRoot.resolve("235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc/v1"),
                "o1v1",
                output);
    }

    @Test
    public void exportObjectWhenObjectHasMutableHead() {
        var repoName = "mutable5";
        var repoRoot = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoRoot);

        var output = outputPath(repoName, "o1");

        repo.exportObject("o1", output);

        ITestHelper.verifyDirectoryContentsSame(
                repoRoot.resolve("235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc"),
                "o1",
                output);
    }

    private Path outputPath(String repoName, String path) {
        try {
            var output = outputDir.resolve(Paths.get(repoName, path));
            Files.createDirectories(output.getParent());
            return output;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
