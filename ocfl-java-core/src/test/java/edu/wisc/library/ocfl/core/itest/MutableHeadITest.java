package edu.wisc.library.ocfl.core.itest;

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorageBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import static edu.wisc.library.ocfl.core.itest.ITestHelper.*;
import static org.junit.jupiter.api.Assertions.*;

public class MutableHeadITest {

    @TempDir
    public Path tempRoot;

    private Path reposDir;
    private Path outputDir;
    private Path inputDir;
    private Path workDir;

    private CommitInfo defaultCommitInfo;

    @BeforeEach
    public void setup() throws IOException {
        reposDir = Files.createDirectory(tempRoot.resolve("repos"));
        outputDir = Files.createDirectory(tempRoot.resolve("output"));
        inputDir = Files.createDirectory(tempRoot.resolve("input"));
        workDir = Files.createDirectory(tempRoot.resolve("work"));

        defaultCommitInfo =  new CommitInfo().setMessage("commit message").setUser(new User().setName("Peter").setAddress("peter@example.com"));

//        var repoDir = FileUtil.createDirectories(Paths.get("src/test/resources/tmp", repoName));
    }

    @Test
    public void createMutableHeadWhenObjectExistsAndDoesNotHaveMutableHead() {
        var repoName = "mutable1";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5" .getBytes()), "file5")
                    .renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void purgeMutableHeadWhenExists() {
        var repoName = "mutable4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5" .getBytes()), "file5")
                    .renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        repo.purgeStagedChanges(objectId);

        assertFalse(repo.hasStagedChanges(objectId));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void createMutableHeadOnNewObject() {
        var repoName = "mutable2";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4")
                    .removeFile("dir1/file3");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void commitStagedVersionWhenHadMutableHeadAndValid() {
        var repoName = "mutable3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4").removeFile("dir1/file3");
        });

        repo.commitStagedChanges(objectId, defaultCommitInfo.setMessage("commit"));

        assertFalse(repo.hasStagedChanges(objectId));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void failWhenCreatingNewVersionOnObjectWithMutableHead() {
        var repoName = "mutable5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.stageChanges(ObjectId.head(objectId), defaultCommitInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3" .getBytes()), "dir1/file3");
        });

        assertThrows(ObjectOutOfSyncException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("update"), updater -> {
                updater.writeFile(new ByteArrayInputStream("file4" .getBytes()), "file4");
            });
        });
    }

    private Path newRepoDir(String name) {
        try {
            return Files.createDirectory(reposDir.resolve(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private MutableOcflRepository defaultRepo(Path repoDir) {
        var repo = new OcflRepositoryBuilder().prettyPrintJson().buildMutable(
                new FileSystemOcflStorageBuilder()
                        .checkNewVersionFixity(true)
                        .build(repoDir, new ObjectIdPathMapperBuilder().buildFlatMapper()),
                workDir);
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

}
