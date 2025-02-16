package io.ocfl.itest;

import static io.ocfl.itest.ITestHelper.expectedRepoPath;
import static io.ocfl.itest.ITestHelper.sourceObjectPath;
import static io.ocfl.itest.ITestHelper.sourceRepoPath;
import static io.ocfl.itest.ITestHelper.streamString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.exception.NotFoundException;
import io.ocfl.api.exception.ObjectOutOfSyncException;
import io.ocfl.api.exception.OcflInputException;
import io.ocfl.api.exception.OcflStateException;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.OcflVersion;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public abstract class MutableHeadITest {

    private static final String O1_PATH =
            "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc";

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

        defaultVersionInfo = new VersionInfo().setMessage("commit message").setUser("Peter", "peter@example.com");

        onBefore();
    }

    @AfterEach
    public void after() {
        onAfter();
    }

    protected MutableOcflRepository defaultRepo(String name) {
        return defaultRepo(name, builder -> builder.defaultLayoutConfig(new HashedNTupleLayoutConfig()));
    }

    protected abstract MutableOcflRepository defaultRepo(String name, Consumer<OcflRepositoryBuilder> consumer);

    protected MutableOcflRepository existingRepo(String name, Path path) {
        return existingRepo(name, path, builder -> builder.defaultLayoutConfig(new HashedNTupleLayoutConfig()));
    }

    protected abstract MutableOcflRepository existingRepo(
            String name, Path path, Consumer<OcflRepositoryBuilder> consumer);

    protected abstract void verifyRepo(String name);

    protected abstract void writeFile(String repoName, String path, InputStream content);

    protected void onBefore() {}

    protected void onAfter() {}

    @Test
    public void createMutableHeadWhenObjectExistsAndDoesNotHaveMutableHead() {
        var repoName = "mutable1";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertFalse(repo.hasStagedChanges(objectId));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5".getBytes()), "file5")
                    .renameFile("dir1/file3", "file3")
                    .removeFile("dir1/file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);

        var details = repo.describeObject(objectId);

        assertTrue(details.getHeadVersion().isMutable(), "HEAD isMutable");
        assertFalse(details.getVersion(VersionNum.fromString("v1")).isMutable(), "v1 isMutable");
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.renameFile("dir1/file3", "file3").removeFile("dir1/file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);

        var details = repo.describeObject(objectId);

        assertTrue(details.getHeadVersion().isMutable(), "HEAD isMutable");
        assertFalse(details.getVersion(VersionNum.fromString("v1")).isMutable(), "v1 isMutable");
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file5".getBytes()), "file5")
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4".getBytes()), "file4")
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4".getBytes()), "file4")
                    .removeFile("dir1/file3");
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3");
        });

        assertThat(
                assertThrows(OcflStateException.class, () -> {
                            repo.updateObject(
                                    ObjectVersionId.head(objectId),
                                    defaultVersionInfo.setMessage("update"),
                                    updater -> {
                                        updater.writeFile(new ByteArrayInputStream("file4".getBytes()), "file4");
                                    });
                        })
                        .getMessage(),
                Matchers.containsString("it has an active mutable HEAD"));
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
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
        });

        OcflAsserts.assertThrowsWithMessage(
                ObjectOutOfSyncException.class, "Changes are out of sync with the current object state", () -> {
                    repo.stageChanges(
                            ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 2"), updater -> {
                                writeFile(
                                        repoName,
                                        O1_PATH + "/extensions/0005-mutable-head/revisions/r2",
                                        TestHelper.inputStream("r2"));
                                updater.writeFile(new ByteArrayInputStream("file5".getBytes()), "file5")
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
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3").removeFile("f1");
        });

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
        });

        OcflAsserts.assertThrowsWithMessage(OcflStateException.class, "has an active mutable HEAD", () -> {
            repo.replicateVersionAsHead(
                    ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("replicate"));
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
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3").removeFile("f1");
        });

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3")
                    .writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file4");
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

    @Test
    public void rejectImportObjectWhenWithMutableHead() {
        var objectId = "o1";
        var repoName1 = "mutable5";
        var repoRoot1 = expectedRepoPath(repoName1);
        var repo1 = existingRepo(repoName1, repoRoot1);

        var output = outputPath(repoName1, objectId);

        repo1.exportObject(objectId, output);

        var repoName2 = "mutable-import";
        var repo2 = defaultRepo(repoName2);

        OcflAsserts.assertThrowsWithMessage(
                OcflInputException.class, "cannot be imported because it contains a mutable HEAD", () -> {
                    repo2.importObject(output);
                });
    }

    @Test
    public void rejectImportObjectWhenWithMutableHeadWhenInvalidMutableHead() throws IOException {
        var objectId = "o1";
        var repoName1 = "mutable5";
        var repoRoot1 = expectedRepoPath(repoName1);
        var repo1 = existingRepo(repoName1, repoRoot1);

        var output = outputPath(repoName1, objectId);

        repo1.exportObject(objectId, output);

        Files.delete(output.resolve("extensions/0005-mutable-head/head/content/r1/dir1/file3"));

        var repoName2 = "mutable-import";
        var repo2 = defaultRepo(repoName2);

        OcflAsserts.assertThrowsWithMessage(
                OcflInputException.class, "cannot be imported because it contains a mutable HEAD", () -> {
                    repo2.importObject(output);
                });
    }

    @Test
    public void rejectUpdateWhenConcurrentChangeToPreviousVersionOfMutableHead() throws InterruptedException {
        var objectId = "o1";

        var repoName = "concurrent-change-mutable";
        var repo = defaultRepo(repoName);

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file1"), "file1.txt");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file2"), "file2.txt");
        });

        var phaser = new Phaser(2);

        var future = CompletableFuture.runAsync(() -> {
            repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
                phaser.arriveAndAwaitAdvance();
                phaser.arriveAndAwaitAdvance();
                updater.writeFile(streamString("file3"), "file3.txt");
            });
        });

        phaser.arriveAndAwaitAdvance();

        repo.rollbackToVersion(ObjectVersionId.version(objectId, "v1"));
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file4"), "file4.txt");
        });

        phaser.arriveAndAwaitAdvance();
        TimeUnit.MILLISECONDS.sleep(100);

        OcflAsserts.assertThrowsWithMessage(
                ObjectOutOfSyncException.class,
                "Cannot update object o1 because the update is out of sync with the current object state. The digest of the current inventory is ",
                () -> {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw e.getCause();
                    }
                });

        var desc = repo.describeObject(objectId);

        assertEquals("v2", desc.getHeadVersionNum().toString());
        assertTrue(desc.getHeadVersion().containsFile("file4.txt"));
        assertFalse(desc.getHeadVersion().containsFile("file3.txt"));
    }

    @Test
    public void rejectUpdateWhenConcurrentChangeWhileCreatingMutableHead() throws InterruptedException {
        var objectId = "o1";

        var repoName = "concurrent-change-mutable-2";
        var repo = defaultRepo(repoName);

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file1"), "file1.txt");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file2"), "file2.txt");
        });

        var phaser = new Phaser(2);

        var future = CompletableFuture.runAsync(() -> {
            repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
                phaser.arriveAndAwaitAdvance();
                phaser.arriveAndAwaitAdvance();
                updater.writeFile(streamString("file3"), "file3.txt");
            });
        });

        phaser.arriveAndAwaitAdvance();

        repo.rollbackToVersion(ObjectVersionId.version(objectId, "v1"));
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file4"), "file4.txt");
        });

        phaser.arriveAndAwaitAdvance();
        TimeUnit.MILLISECONDS.sleep(100);

        OcflAsserts.assertThrowsWithMessage(
                ObjectOutOfSyncException.class,
                "Cannot update object o1 because the update is out of sync with the current object state. The digest of the current inventory is ",
                () -> {
                    try {
                        future.get();
                    } catch (ExecutionException e) {
                        throw e.getCause();
                    }
                });

        var desc = repo.describeObject(objectId);

        assertEquals("v2", desc.getHeadVersionNum().toString());
        assertTrue(desc.getHeadVersion().containsFile("file4.txt"));
        assertFalse(desc.getHeadVersion().containsFile("file3.txt"));
    }

    @Test
    public void upgradeOcflObjectOnCommit() {
        var repoName = "mutable7";
        var repo = defaultRepo(
                repoName, builder -> builder.ocflConfig(config -> config.setOcflVersion(OcflVersion.OCFL_1_0)));

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 1"), "file1.txt");
        });

        repo = defaultRepo(
                repoName,
                builder -> builder.ocflConfig(
                        config -> config.setOcflVersion(OcflVersion.OCFL_1_1).setUpgradeObjectsOnWrite(true)));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 2"), "file2.txt");
        });

        assertEquals(OcflVersion.OCFL_1_0, repo.describeObject(objectId).getObjectOcflVersion());

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 3"), "file3.txt");
        });

        repo.commitStagedChanges(objectId, defaultVersionInfo.setMessage("commit"));

        assertEquals(OcflVersion.OCFL_1_1, repo.describeObject(objectId).getObjectOcflVersion());

        verifyRepo(repoName);
    }

    @Test
    public void doNotUpgradeOcflObjectOnCommitWhenUpgradeDisabled() {
        var repoName = "mutable8";
        var repo = defaultRepo(
                repoName, builder -> builder.ocflConfig(config -> config.setOcflVersion(OcflVersion.OCFL_1_0)));

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 1"), "file1.txt");
        });

        repo = defaultRepo(
                repoName,
                builder -> builder.ocflConfig(
                        config -> config.setOcflVersion(OcflVersion.OCFL_1_1).setUpgradeObjectsOnWrite(false)));

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 2"), "file2.txt");
        });

        assertEquals(OcflVersion.OCFL_1_0, repo.describeObject(objectId).getObjectOcflVersion());

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(streamString("file 3"), "file3.txt");
        });

        repo.commitStagedChanges(objectId, defaultVersionInfo.setMessage("commit"));

        assertEquals(OcflVersion.OCFL_1_0, repo.describeObject(objectId).getObjectOcflVersion());

        verifyRepo(repoName);
    }

    @Test
    public void cleanupOldRevisionMarkers() {
        var objectId = "o1";
        var repoName = "mutable9";
        var repoRoot = sourceRepoPath(repoName);
        var repo = existingRepo(repoName, repoRoot);

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("update"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file4".getBytes()), "file4");
        });

        assertTrue(repo.hasStagedChanges(objectId));

        verifyRepo(repoName);
    }

    @Test
    public void stageIdenticalFileToFileAlreadyStaged() {
        var repoName = "mutable10";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "dir1/file3");
        });
        repo.stageChanges(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("stage 1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("file3".getBytes()), "file4");
        });

        verifyRepo(repoName);
    }

    @Test
    public void stageFileWithContentAlreadyPresentInObject() throws IOException {
        var repoName = "mutable11";
        var repo = defaultRepo(repoName);

        String objectId = "object_1";
        ObjectVersionId head = ObjectVersionId.head(objectId);
        repo.stageChanges(head, new VersionInfo(), (updater) -> {
            updater.writeFile(new ByteArrayInputStream(new byte[] {1}), "info_1.txt");
        });
        repo.commitStagedChanges(objectId, new VersionInfo());

        repo.stageChanges(head, new VersionInfo(), (updater) -> {
            updater.writeFile(new ByteArrayInputStream(new byte[] {1}), "info_2.txt");
        });
        repo.commitStagedChanges(objectId, new VersionInfo());

        verifyRepo(repoName);
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
