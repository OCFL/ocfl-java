package edu.wisc.library.ocfl.itest;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.api.model.VersionId;
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

import static edu.wisc.library.ocfl.itest.ITestHelper.sourceObjectPath;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public abstract class MutableHeadITest {

    private static final String O1_PATH = "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc";

    @TempDir
    public Path tempRoot;

    protected Path reposDir;
    protected Path workDir;

    protected VersionInfo defaultVersionInfo;

    @BeforeEach
    public void setup() throws IOException {
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

    protected abstract MutableOcflRepository defaultRepo(String name);

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

}
