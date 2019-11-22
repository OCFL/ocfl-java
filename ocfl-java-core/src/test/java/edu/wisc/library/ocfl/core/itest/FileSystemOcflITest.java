package edu.wisc.library.ocfl.core.itest;

import edu.wisc.library.ocfl.api.OcflObjectVersionFile;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.exception.*;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorageBuilder;
import edu.wisc.library.ocfl.core.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Map;

import static edu.wisc.library.ocfl.core.itest.ITestHelper.*;
import static edu.wisc.library.ocfl.core.matcher.OcflMatchers.commitInfo;
import static edu.wisc.library.ocfl.core.matcher.OcflMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

public class FileSystemOcflITest {

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

        defaultCommitInfo = ITestHelper.commitInfo("Peter", "peter@example.com", "commit message");
    }

    @Test
    public void putNewObjectAndUpdateMultipleTimesWithAdditionalPuts() {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        repo.getObject(ObjectVersionId.head(objectId), outputPath1);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o1v3"), objectId, outputPath1);

        repo.getObject(ObjectVersionId.version(objectId, "v2"), outputPath2);
        verifyDirectoryContentsSame(sourcePathV2, outputPath2.getFileName().toString(), outputPath2);

        repo.getObject(ObjectVersionId.version(objectId, "v1"), outputPath3);
        verifyDirectoryContentsSame(sourcePathV1, outputPath3.getFileName().toString(), outputPath3);
    }

    @Test
    public void putObjectWithPathToSingleFile() {
        var repoName = "repo15";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1.resolve("file1"), defaultCommitInfo);
        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void rejectRequestsWhenRepoClosed() {
        var repoName = "repo15";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        repo.close();

        var objectId = "o1";
        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        assertThat(assertThrows(IllegalStateException.class, () -> {
            repo.putObject(ObjectVersionId.head(objectId), sourcePathV1.resolve("file1"), defaultCommitInfo);
        }).getMessage(), containsString("is closed"));
    }

    @Test
    public void updateObjectMakeMultipleChangesWithinTheSameVersion() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(input(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        repo.getObject(ObjectVersionId.head(objectId), outputPath1);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v3"), objectId, outputPath1);

        repo.getObject(ObjectVersionId.version(objectId, "v2"), outputPath2);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v2"), outputPath2.getFileName().toString(), outputPath2);

        repo.getObject(ObjectVersionId.version(objectId, "v1"), outputPath3);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v1"), outputPath3.getFileName().toString(), outputPath3);
    }

    @Test
    public void lazyLoadObject() throws IOException {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        var files = repo.getObject(ObjectVersionId.head(objectId));
        assertEquals(2, files.getFiles().size());
        verifyStream(sourcePathV3.resolve("file2"), files.getFile("file2"));
        verifyStream(sourcePathV3.resolve("file4"), files.getFile("file4"));

        files = repo.getObject(ObjectVersionId.version(objectId, "v2"));
        assertEquals(3, files.getFiles().size());
        verifyStream(sourcePathV2.resolve("file1"), files.getFile("file1"));
        verifyStream(sourcePathV2.resolve("file2"), files.getFile("file2"));
        verifyStream(sourcePathV2.resolve("dir1/file3"), files.getFile("dir1/file3"));


        files = repo.getObject(ObjectVersionId.version(objectId, "v1"));
        assertEquals(2, files.getFiles().size());
        verifyStream(sourcePathV1.resolve("file1"), files.getFile("file1"));
        verifyStream(sourcePathV1.resolve("file2"), files.getFile("file2"));
    }

    @Test
    public void renameAndRemoveFilesAddedInTheCurrentVersion() {
        var repoName = "repo17";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("file2"), "dir1/file2")
                    .addPath(sourcePathV2.resolve("file3"), "file3")
                    .renameFile("dir1/file2", "dir2/file3");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.writeFile(new ByteArrayInputStream("test123".getBytes()), "file4")
                    .removeFile("file4");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.writeFile(new ByteArrayInputStream("test123456".getBytes()), "file5")
                    .writeFile(new ByteArrayInputStream("6543210".getBytes()), "file5", OcflOption.OVERWRITE);
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void describeObject() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"))
                    .addFileFixity("file1", DigestAlgorithm.md5, "95efdf0764d92207b4698025f2518456")
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                    .addFileFixity("file1", DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669")
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")
                    .addFileFixity("dir1/file3", DigestAlgorithm.md5, "72b6193fe19ec99c692eba5c798e6bdf");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v3"))
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")
                    .addFileFixity("file4", DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669");
        });

        var objectDetails = repo.describeObject(objectId);

        assertEquals(objectId, objectDetails.getId());
        assertEquals(VersionId.fromString("v3"), objectDetails.getHeadVersionId());
        assertEquals(3, objectDetails.getVersionMap().size());

        assertThat(objectDetails.getVersion(VersionId.fromString("v1")), versionDetails(objectId, "v1",
                commitInfo(defaultCommitInfo.getUser(), "1"),
                fileDetails("file1", "o1/v1/content/file1", Map.of(
                        DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e",
                        DigestAlgorithm.md5, "95efdf0764d92207b4698025f2518456")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v2")), versionDetails(objectId, "v2",
                commitInfo(defaultCommitInfo.getUser(), "2"),
                fileDetails("file1", "o1/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("dir1/file3", "o1/v2/content/dir1/file3", Map.of(
                        DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2",
                        DigestAlgorithm.md5, "72b6193fe19ec99c692eba5c798e6bdf"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v3")), versionDetails(objectId, "v3",
                commitInfo(defaultCommitInfo.getUser(), "3"),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("file4", "o1/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669"))
        ));

        assertSame(objectDetails.getHeadVersion(), objectDetails.getVersion(VersionId.fromString("v3")));
    }

    @Test
    public void shouldNotAddAdditionalFixityWhenDefaultAlgorithmSpecified() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"))
                    .addFileFixity("file1", DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e")
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                    .addFileFixity("file1", DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc")
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")
                    .addFileFixity("dir1/file3", DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v3"))
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")
                    .addFileFixity("file4", DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc");
        });

        var objectDetails = repo.describeObject(objectId);

        assertEquals(objectId, objectDetails.getId());
        assertEquals(VersionId.fromString("v3"), objectDetails.getHeadVersionId());
        assertEquals(3, objectDetails.getVersionMap().size());

        assertThat(objectDetails.getVersion(VersionId.fromString("v1")), versionDetails(objectId, "v1",
                commitInfo(defaultCommitInfo.getUser(), "1"),
                fileDetails("file1", "o1/v1/content/file1", Map.of(
                        DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v2")), versionDetails(objectId, "v2",
                commitInfo(defaultCommitInfo.getUser(), "2"),
                fileDetails("file1", "o1/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                fileDetails("dir1/file3", "o1/v2/content/dir1/file3", Map.of(
                        DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v3")), versionDetails(objectId, "v3",
                commitInfo(defaultCommitInfo.getUser(), "3"),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                fileDetails("file4", "o1/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc"))
        ));

        assertSame(objectDetails.getHeadVersion(), objectDetails.getVersion(VersionId.fromString("v3")));
    }

    @Test
    public void shouldFailWhenFixityDoesNotMatch() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        OcflAsserts.assertThrowsWithMessage(FixityCheckException.class, "Expected md5 digest of", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
                updater.addPath(sourceObjectPath(objectId, "v1"))
                        .addFileFixity("file1", DigestAlgorithm.md5, "bogus");
            });
        });
    }

    @Test
    public void shouldFailFixityWhenUnknownAlgorithm() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "specified digest algorithm is not mapped to a Java name", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
                updater.addPath(sourceObjectPath(objectId, "v1"))
                        .addFileFixity("file1", DigestAlgorithm.fromOcflName("bogus"), "bogus");
            });
        });
    }

    @Test
    public void shouldFailFixityWhenFileNotAddedInBlockAndDoesNotHaveExistingFixity() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"));
        });

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "not newly added in the current block", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                        .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad");
            });
        });
    }

    @Test
    public void readObjectFiles() {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var ocflObject = repo.getObject(ObjectVersionId.head(objectId));

        assertThat(ocflObject, objectVersion(objectId, "v3",
                commitInfo(defaultCommitInfo.getUser(), "3"),
                versionFile("dir1/dir2/file2", "o2/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("dir1/file3", "o2/v3/content/dir1/file3",
                        "This is a different file 3",
                        Map.of(DigestAlgorithm.sha512, "6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331"))
                ));

        ocflObject = repo.getObject(ObjectVersionId.version(objectId, "v1"));

        assertThat(ocflObject, objectVersion(objectId, "v1",
                commitInfo(defaultCommitInfo.getUser(), "commit message"),
                versionFile("dir1/dir2/file2", "o2/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("file1", "o2/v1/content/file1",
                        "Test file 1",
                        Map.of(DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e"))
                ));
    }

    @Test
    public void getObjectFilesFromLazyLoadGetObject() {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var objectVersion = repo.getObject(ObjectVersionId.head(objectId));

        assertThat(objectVersion, objectVersion(objectId, "v3",
                commitInfo(defaultCommitInfo.getUser(), "3"),
                versionFile("dir1/dir2/file2", "o2/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("dir1/file3", "o2/v3/content/dir1/file3",
                        "This is a different file 3",
                        Map.of(DigestAlgorithm.sha512, "6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331"))
        ));

        objectVersion = repo.getObject(ObjectVersionId.version(objectId, "v1"));

        assertThat(objectVersion, objectVersion(objectId, "v1",
                commitInfo(defaultCommitInfo.getUser(), defaultCommitInfo.getMessage()),
                versionFile("dir1/dir2/file2", "o2/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("file1", "o2/v1/content/file1",
                        "Test file 1",
                        Map.of(DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e"))
        ));
    }

    @Test
    public void putObjectWithNoFiles() throws IOException {
        var repoName = "repo6";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o4";

        var empty = Files.createDirectory(tempRoot.resolve("empty"));

        repo.putObject(ObjectVersionId.head(objectId), empty, defaultCommitInfo);

        var details = repo.describeObject(objectId);

        assertEquals(1, details.getVersionMap().size());
        assertEquals(0, details.getHeadVersion().getFiles().size());

        var outputPath = outputPath(repoName, objectId);

        repo.getObject(ObjectVersionId.head(objectId), outputPath);

        assertEquals(0, outputPath.toFile().list().length);
    }

    @Test
    public void removeAllOfTheFilesFromAnObject() throws IOException {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        repo.putObject(ObjectVersionId.head(objectId), sourceObjectPath(objectId, "v1"), defaultCommitInfo);

        var ocflObject = repo.getObject(ObjectVersionId.head(objectId));

        repo.updateObject(ocflObject.getObjectVersionId(), defaultCommitInfo.setMessage("delete content"), updater -> {
            ocflObject.getFiles().forEach(file -> {
                updater.removeFile(file.getPath());
            });
        });

        var outputPath = outputPath(repoName, objectId);
        repo.getObject(ObjectVersionId.head(objectId), outputPath);
        assertEquals(0, outputPath.toFile().list().length);
    }

    @Test
    public void rejectInvalidObjectIds() throws IOException {
        var repoName = "repo6";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var empty = Files.createDirectory(tempRoot.resolve("empty"));

        assertThrows(IllegalArgumentException.class, () -> repo.putObject(ObjectVersionId.head(".."), empty, defaultCommitInfo));
    }

    @Test
    public void rejectObjectNotFoundWhenObjectDoesNotExists() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectVersionId.head("bogus"), outputPath(repoName, "bogus")));
    }

    @Test
    public void rejectObjectNotFoundWhenObjectExistsButVersionDoesNot() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectVersionId.version(objectId, "v100"), outputPath(repoName, objectId)));
    }

    @Test
    public void shouldUpdateObjectWhenReferenceVersionSpecifiedAndIsMostRecentVersion() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.version(objectId, "v2"), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(input(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void rejectUpdateObjectWhenReferenceVersionSpecifiedAndIsNotMostRecentVersion() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        assertThrows(ObjectOutOfSyncException.class, () -> repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(input(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        }));
    }

    @Test
    public void shouldCreateNewVersionWhenObjectUpdateWithNoChanges() {
        var repoName = "repo7";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.putObject(ObjectVersionId.head(objectId), sourceObjectPath(objectId, "v1"), defaultCommitInfo.setMessage("1"));
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            // no op
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void failGetObjectWhenInventoryFixityCheckFails() {
        var repoName = "invalid-inventory-fixity";
        var repoDir = sourceRepoPath(repoName);
        assertThrows(FixityCheckException.class, () -> defaultRepo(repoDir));
    }

    @Test
    public void failToInitRepoWhenObjectsStoredUsingDifferentLayout() {
        var repoName = "repo3";
        var repoDir = expectedRepoPath(repoName);
        assertThrows(IllegalStateException.class, () -> {
            new OcflRepositoryBuilder().inventoryMapper(ITestHelper.testInventoryMapper()).build(
                    new FileSystemOcflStorage(repoDir, new ObjectIdPathMapperBuilder().buildDefaultPairTreeMapper()),
                    repoDir.resolve("deposit"));
        });
    }

    @Test
    public void failGetObjectWhenFileFixityCheckFails() {
        var repoName = "invalid-file-fixity";
        var repoDir = sourceRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        assertThrows(FixityCheckException.class, () -> repo.getObject(ObjectVersionId.head("o1"), outputPath(repoName, "blah")));
    }

    @Test
    public void failGetObjectWhenInvalidDigestAlgorithmUsed() {
        var repoName = "invalid-digest-algorithm";
        var repoDir = sourceRepoPath(repoName);
        assertThat(assertThrows(RuntimeIOException.class, () -> {
            defaultRepo(repoDir);
        }).getMessage(), containsString("digestAlgorithm must be sha512 or sha256"));
    }

    @Test
    public void putObjectWithDuplicateFiles() {
        var repoName = "repo8";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o5";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        // Which duplicate file that's preserved is non-deterministic
        var expectedPaths = ITestHelper.listAllPaths(expectedRepoPath(repoName));
        var actualPaths = ITestHelper.listAllPaths(repoDir);
        assertEquals(expectedPaths.size(), actualPaths.size());
    }

    @Test
    public void useZeroPaddedVersionsWhenExistingVersionIsZeroPadded() {
        var repoName = "zero-padded";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void useDifferentContentDirectoryWhenExistingObjectIsUsingDifferentDir() {
        var repoName = "different-content";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldUseDifferentDigestAlgorithmWhenInventoryHasDifferent() {
        var repoName = "different-digest";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void allowPathsWithDifficultCharsWhenNoRestrictionsApplied() throws IOException {
        var repoName = "repo16";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("test1".getBytes()), "backslash\\path\\file");
            updater.writeFile(new ByteArrayInputStream("test3".getBytes()), "fi\u0080le");
        });

        var expectedRepoPath = expectedRepoPath(repoName);
        var backslashFile = expectedRepoPath.resolve("o1/v1/content/backslash\\path\\file");
        try {
            Files.write(backslashFile, "test1".getBytes());
            verifyDirectoryContentsSame(expectedRepoPath, repoDir);
        } finally {
            Files.deleteIfExists(backslashFile);
        }
    }

    @Test
    public void rejectPathsWhenInvalidAndNotSanitized() {
        var repoName = "repo9";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test2".getBytes()), "file/");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "/absolute/path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "empty//path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "relative/../../path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "./file");
            });
        });
    }

    @Test
    public void reinstateFileThatWasRemoved() {
        var repoName = "repo9";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.removeFile("file3");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file3", "file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void reinstateFileThatWasNeverRemoved() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file2", "file1");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldRejectReinstateWhenVersionDoesNotExist() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThat(assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.reinstateFile(VersionId.fromString("v3"), "file2", "file1");
            });
        }).getMessage(), containsString("does not contain a file at"));
    }

    @Test
    public void shouldRejectReinstateWhenFileDoesNotExist() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.reinstateFile(VersionId.fromString("v1"), "file4", "file1");
            });
        });
    }

    @Test
    public void purgeObjectWhenExists() {
        var repoName = "purge-object";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.purgeObject(objectId);

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject(objectId);
        });

        assertEquals(4, ITestHelper.listAllPaths(repoDir).size());
    }

    @Test
    public void purgeObjectDoNothingWhenDoesNotExist() {
        var repoName = "purge-object";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.purgeObject("o4");

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject("o4");
        });

        assertEquals(14, ITestHelper.listAllPaths(repoDir).size());
    }

    @Test
    public void shouldCreateNewObjectWithUpdateObjectApi() {
        var repoName = "repo11";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
            updater.addPath(sourcePath.resolve("file2"), "file2")
                    .addPath(sourcePath.resolve("file3"), "file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldReturnObjectExistence() {
        var repoName = "repo11";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePath, defaultCommitInfo.setMessage("1"));

        assertTrue(repo.containsObject(objectId));
        assertFalse(repo.containsObject("o4"));
    }

    @Test
    public void shouldMoveFilesIntoRepoOnPutObjectWhenMoveSourceSpecified() {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o1";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo, OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"), OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"), OcflOption.MOVE_SOURCE);

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
        assertFalse(Files.exists(sourcePathV1));
        assertFalse(Files.exists(sourcePathV2));
        assertFalse(Files.exists(sourcePathV3));
    }

    @Test
    public void shouldMoveFilesIntoRepoOnUpdateObjectWhenMoveSourceSpecified() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3", OcflOption.MOVE_SOURCE)
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .addPath(sourcePathV3.resolve("dir1"), "dir1", OcflOption.MOVE_SOURCE);
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
        assertEquals(2, ITestHelper.listAllPaths(sourcePathV2).size());
        assertEquals(1, ITestHelper.listAllPaths(sourcePathV3).size());
    }

    @Test
    public void shouldMoveSrcDirContentsIntoSubdirWhenSubdirSpecifiedAsDst() {
        var repoName = "repo12";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV1, "sub", OcflOption.MOVE_SOURCE);
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV2, "sub", OcflOption.MOVE_SOURCE);
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldAddFileWithFileNameWhenNoDestinationGiven() {
        var repoName = "repo13";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV1.resolve("file1"));
        });

        var files = repo.describeVersion(ObjectVersionId.head(objectId)).getFiles();

        assertEquals(1, files.size());
        assertEquals("file1", files.iterator().next().getPath());
    }

    @Test
    public void addDirectoryToRootWhenDestinationNotSpecified() {
        var repoName = "repo13";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV1, "");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void writeInputStreamToObjectWhenHasFixityCheckAndValid() {
        var repoName = "repo14";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
            updater.writeFile(
                    new FixityCheckInputStream(input(sourcePath.resolve("file1")), DigestAlgorithm.md5.getJavaStandardName(), "95efdf0764d92207b4698025f2518456"),
                    "file1");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void failInputStreamToObjectWhenHasFixityCheckAndNotValid() {
        var repoName = "repo14";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        assertThrows(FixityCheckException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultCommitInfo, updater -> {
                updater.writeFile(
                        new FixityCheckInputStream(input(sourcePath.resolve("file1")), DigestAlgorithm.md5.getJavaStandardName(), "bogus"),
                        "file1");
            });
        });
    }

    private void verifyStream(Path expectedFile, OcflObjectVersionFile actual) throws IOException {
        var stream = actual.getStream();
        var contents = ITestHelper.inputToString(stream);
        stream.checkFixity();
        assertEquals(ITestHelper.inputToString(Files.newInputStream(expectedFile)), contents);
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

    private Path newRepoDir(String name) {
        try {
            return Files.createDirectory(reposDir.resolve(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private InputStream input(Path path) {
        try {
            return Files.newInputStream(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private OcflRepository defaultRepo(Path repoDir) {
        var repo = new OcflRepositoryBuilder().inventoryMapper(ITestHelper.testInventoryMapper()).build(
                new FileSystemOcflStorageBuilder()
                        .checkNewVersionFixity(true)
                        .objectMapper(ITestHelper.prettyPrintMapper())
                        .build(repoDir, new ObjectIdPathMapperBuilder().buildFlatMapper()),
                workDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");
        return repo;
    }

    private Path copyDir(Path source, Path target) {
        try (var files = Files.walk(source)) {
            files.forEach(f -> {
                try {
                    Files.copy(f, target.resolve(source.relativize(f)), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return target;
    }

    private void printFiles(Path path) {
        try {
            Files.walk(path).forEach(System.out::println);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ;
    }

}
