package edu.wisc.library.ocfl.itest;

import edu.wisc.library.ocfl.api.OcflObjectVersionFile;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatLayoutConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;
import edu.wisc.library.ocfl.test.OcflAsserts;
import edu.wisc.library.ocfl.test.TestHelper;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;

import static edu.wisc.library.ocfl.itest.ITestHelper.*;
import static edu.wisc.library.ocfl.test.TestHelper.copyDir;
import static edu.wisc.library.ocfl.test.TestHelper.inputStream;
import static edu.wisc.library.ocfl.test.matcher.OcflMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.*;

public abstract class OcflITest {

    private static final String O1_PATH = "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc";
    private static final String O2_PATH = "925/0b9/912/9250b9912ee91d6b46e23299459ecd6eb8154451d62558a3a0a708a77926ad04";

    @TempDir
    public Path tempRoot;

    protected Path outputDir;
    protected Path inputDir;
    protected Path workDir;

    protected VersionInfo defaultVersionInfo;

    @BeforeEach
    public void setup() throws IOException {
        outputDir = Files.createDirectory(tempRoot.resolve("output"));
        inputDir = Files.createDirectory(tempRoot.resolve("input"));
        workDir = Files.createDirectory(tempRoot.resolve("work"));

        defaultVersionInfo = new VersionInfo().setMessage("commit message").setUser("Peter", "peter@example.com");

        onBefore();
    }

    @AfterEach
    public void after() {
        onAfter();
    }

    protected OcflRepository defaultRepo(String name) {
        return defaultRepo(name, new HashedTruncatedNTupleConfig());
    }

    protected abstract OcflRepository defaultRepo(String name, OcflExtensionConfig layoutConfig);

    protected OcflRepository existingRepo(String name, Path path) {
        return existingRepo(name, path, new HashedTruncatedNTupleConfig());
    }

    protected abstract OcflRepository existingRepo(String name, Path path, OcflExtensionConfig layoutConfig);

    protected abstract void verifyRepo(String name);

    protected abstract List<String> listFilesInRepo(String name);

    protected void onBefore() {

    }

    protected void onAfter() {

    }

    @Test
    public void putNewObjectAndUpdateMultipleTimesWithAdditionalPuts() {
        var repoName = "repo3";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"));
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultVersionInfo.setMessage("third"));

        verifyRepo(repoName);

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
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1.resolve("file1"), defaultVersionInfo);
        verifyRepo(repoName);
    }

    @Test
    public void shouldNotFailWhenObjectIdLongerThan255Characters() {
        var repoName = "long-id";
        var repo = defaultRepo(repoName, new HashedTruncatedNTupleConfig());

        var objectId = "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789" +
                "0123456789012345678901234567890123456789012345678901234567890123456789012345678901234567890123456789";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(inputStream("long-id"), "file1.txt");
        });

        var object = repo.getObject(ObjectVersionId.head(objectId));

        assertEquals(objectId, object.getObjectId());
    }

    @Test
    public void rejectRequestsWhenRepoClosed() {
        var repoName = "repo15";
        var repo = defaultRepo(repoName);

        repo.close();

        var objectId = "o1";
        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        assertThat(assertThrows(IllegalStateException.class, () -> {
            repo.putObject(ObjectVersionId.head(objectId), sourcePathV1.resolve("file1"), defaultVersionInfo);
        }).getMessage(), containsString("is closed"));
    }

    @Test
    public void updateObjectMakeMultipleChangesWithinTheSameVersion() {
        var repoName = "repo4";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(inputStream(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        });

        verifyRepo(repoName);

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
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"));
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultVersionInfo.setMessage("third"));

        verifyRepo(repoName);

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
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("file2"), "dir1/file2")
                    .addPath(sourcePathV2.resolve("file3"), "file3")
                    .renameFile("dir1/file2", "dir2/file3");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.writeFile(new ByteArrayInputStream("test123".getBytes()), "file4")
                    .removeFile("file4");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.writeFile(new ByteArrayInputStream("test123456".getBytes()), "file5")
                    .writeFile(new ByteArrayInputStream("6543210".getBytes()), "file5", OcflOption.OVERWRITE);
        });

        verifyRepo(repoName);
    }

    @Test
    public void describeObject() {
        var repoName = "repo5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"))
                    .addFileFixity("file1", DigestAlgorithm.md5, "95efdf0764d92207b4698025f2518456")
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                    .addFileFixity("file1", DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669")
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")
                    .addFileFixity("dir1/file3", DigestAlgorithm.md5, "72b6193fe19ec99c692eba5c798e6bdf");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v3"))
                    .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")
                    .addFileFixity("file4", DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669");
        });

        var objectDetails = repo.describeObject(objectId);

        assertEquals(objectId, objectDetails.getId());
        assertEquals(VersionId.fromString("v3"), objectDetails.getHeadVersionId());
        assertEquals(DigestAlgorithm.sha512, objectDetails.getDigestAlgorithm());
        assertEquals(3, objectDetails.getVersionMap().size());

        assertThat(objectDetails.getVersion(VersionId.fromString("v1")), versionDetails(objectId, "v1",
                versionInfo(defaultVersionInfo.getUser(), "1"),
                fileDetails("file1", O1_PATH + "/v1/content/file1", Map.of(
                        DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e",
                        DigestAlgorithm.md5, "95efdf0764d92207b4698025f2518456")),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v2")), versionDetails(objectId, "v2",
                versionInfo(defaultVersionInfo.getUser(), "2"),
                fileDetails("file1", O1_PATH + "/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669")),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("dir1/file3", O1_PATH + "/v2/content/dir1/file3", Map.of(
                        DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2",
                        DigestAlgorithm.md5, "72b6193fe19ec99c692eba5c798e6bdf"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v3")), versionDetails(objectId, "v3",
                versionInfo(defaultVersionInfo.getUser(), "3"),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("file4", O1_PATH + "/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        DigestAlgorithm.md5, "a0a8bfbf51b81caf7aa5be00f5e26669"))
        ));

        assertSame(objectDetails.getHeadVersion(), objectDetails.getVersion(VersionId.fromString("v3")));
    }

    @Test
    public void shouldNotAddAdditionalFixityWhenDefaultAlgorithmSpecified() {
        var repoName = "repo5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"))
                    .addFileFixity("file1", DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e")
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                    .addFileFixity("file1", DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc")
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")
                    .addFileFixity("dir1/file3", DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.clearVersionState().addPath(sourceObjectPath(objectId, "v3"))
                    .addFileFixity("file2", DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")
                    .addFileFixity("file4", DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc");
        });

        var objectDetails = repo.describeObject(objectId);

        assertEquals(objectId, objectDetails.getId());
        assertEquals(VersionId.fromString("v3"), objectDetails.getHeadVersionId());
        assertEquals(3, objectDetails.getVersionMap().size());

        assertThat(objectDetails.getVersion(VersionId.fromString("v1")), versionDetails(objectId, "v1",
                versionInfo(defaultVersionInfo.getUser(), "1"),
                fileDetails("file1", O1_PATH + "/v1/content/file1", Map.of(
                        DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e")),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v2")), versionDetails(objectId, "v2",
                versionInfo(defaultVersionInfo.getUser(), "2"),
                fileDetails("file1", O1_PATH + "/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc")),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                fileDetails("dir1/file3", O1_PATH + "/v2/content/dir1/file3", Map.of(
                        DigestAlgorithm.sha512, "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2"))
        ));

        assertThat(objectDetails.getVersion(VersionId.fromString("v3")), versionDetails(objectId, "v3",
                versionInfo(defaultVersionInfo.getUser(), "3"),
                fileDetails("file2", O1_PATH + "/v1/content/file2", Map.of(
                        DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                fileDetails("file4", O1_PATH + "/v2/content/file1", Map.of(
                        DigestAlgorithm.sha512, "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc"))
        ));

        assertSame(objectDetails.getHeadVersion(), objectDetails.getVersion(VersionId.fromString("v3")));
    }

    @Test
    public void shouldFailWhenFixityDoesNotMatch() {
        var repoName = "repo5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        OcflAsserts.assertThrowsWithMessage(FixityCheckException.class, "Expected md5 digest of", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
                updater.addPath(sourceObjectPath(objectId, "v1"))
                        .addFileFixity("file1", DigestAlgorithm.md5, "bogus");
            });
        });
    }

    @Test
    public void shouldFailFixityWhenUnknownAlgorithm() {
        var repoName = "repo5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "specified digest algorithm is not mapped to a Java name", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
                updater.addPath(sourceObjectPath(objectId, "v1"))
                        .addFileFixity("file1", DigestAlgorithm.fromOcflName("bogus"), "bogus");
            });
        });
    }

    @Test
    public void shouldFailFixityWhenFileNotAddedInBlockAndDoesNotHaveExistingFixity() {
        var repoName = "repo5";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.addPath(sourceObjectPath(objectId, "v1"));
        });

        OcflAsserts.assertThrowsWithMessage(IllegalStateException.class, "not newly added in the current block", () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
                updater.clearVersionState().addPath(sourceObjectPath(objectId, "v2"))
                        .addFileFixity("file2", DigestAlgorithm.md5, "55c1824fcae2b1b51cef5037405fc1ad");
            });
        });
    }

    @Test
    public void readObjectFiles() {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        var objectId = "o2";

        var ocflObject = repo.getObject(ObjectVersionId.head(objectId));

        assertThat(ocflObject, objectVersion(objectId, "v3",
                versionInfo(defaultVersionInfo.getUser(), "3"),
                versionFile("dir1/dir2/file2", O2_PATH + "/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("dir1/file3", O2_PATH + "/v3/content/dir1/file3",
                        "This is a different file 3",
                        Map.of(DigestAlgorithm.sha512, "6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331"))
                ));

        ocflObject = repo.getObject(ObjectVersionId.version(objectId, "v1"));

        assertThat(ocflObject, objectVersion(objectId, "v1",
                versionInfo(defaultVersionInfo.getUser(), "commit message"),
                versionFile("dir1/dir2/file2", O2_PATH + "/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("file1", O2_PATH + "/v1/content/file1",
                        "Test file 1",
                        Map.of(DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e"))
                ));
    }

    @Test
    public void changeHistory() {
        var repo = defaultRepo("change-history");

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3")
                    .removeFile("f1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        var f1History = repo.fileChangeHistory(objectId, "f1");
        var f2History = repo.fileChangeHistory(objectId, "f2");
        var f3History = repo.fileChangeHistory(objectId, "f3");

        assertThat(f1History.getFileChanges(), contains(
                fileChange(FileChangeType.UPDATE,
                        ObjectVersionId.version(objectId, "v1"),
                        "f1", O1_PATH + "/v1/content/f1",
                        versionInfo(defaultVersionInfo.getUser(), "1"),
                        Map.of(DigestAlgorithm.sha512, "4dff4ea340f0a823f15d3f4f01ab62eae0e5da579ccb851f8db9dfe84c58b2b37b89903a740e1ee172da793a6e79d560e5f7f9bd058a12a280433ed6fa46510a")),
                fileChange(FileChangeType.REMOVE,
                        ObjectVersionId.version(objectId, "v2"),
                        "f1", null,
                        versionInfo(defaultVersionInfo.getUser(), "2"),
                        Map.of()),
                fileChange(FileChangeType.UPDATE,
                        ObjectVersionId.version(objectId, "v3"),
                        "f1", O1_PATH + "/v1/content/f1",
                        versionInfo(defaultVersionInfo.getUser(), "3"),
                        Map.of(DigestAlgorithm.sha512, "4dff4ea340f0a823f15d3f4f01ab62eae0e5da579ccb851f8db9dfe84c58b2b37b89903a740e1ee172da793a6e79d560e5f7f9bd058a12a280433ed6fa46510a"))));

        assertThat(f2History.getFileChanges(), contains(
                fileChange(FileChangeType.UPDATE,
                        ObjectVersionId.version(objectId, "v1"),
                        "f2", O1_PATH + "/v1/content/f2",
                        versionInfo(defaultVersionInfo.getUser(), "1"),
                        Map.of(DigestAlgorithm.sha512, "40b244112641dd78dd4f93b6c9190dd46e0099194d5a44257b7efad6ef9ff4683da1eda0244448cb343aa688f5d3efd7314dafe580ac0bcbf115aeca9e8dc114")),
                fileChange(FileChangeType.UPDATE,
                        ObjectVersionId.version(objectId, "v3"),
                        "f2", O1_PATH + "/v3/content/f2",
                        versionInfo(defaultVersionInfo.getUser(), "3"),
                        Map.of(DigestAlgorithm.sha512, "7db70149dac5561e411a202629d06832b06b7e8dfef61086ff9e0922459fbe14a69d565cf838fd43681fdb29a698bfe377861b966d12416298997843820bfdb7"))));

        assertThat(f3History.getFileChanges(), contains(
                fileChange(FileChangeType.UPDATE,
                        ObjectVersionId.version(objectId, "v2"),
                        "f3", O1_PATH + "/v2/content/f3",
                        versionInfo(defaultVersionInfo.getUser(), "2"),
                        Map.of(DigestAlgorithm.sha512, "3bafbf08882a2d10133093a1b8433f50563b93c14acd05b79028eb1d12799027241450980651994501423a66c276ae26c43b739bc65c4e16b10c3af6c202aebb"))));
    }

    @Test
    public void failWhenLogicalPathNotFoundInChangeHistory() {
        var repo = defaultRepo("change-history");

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.writeFile(new ByteArrayInputStream("3".getBytes()), "f3")
                    .removeFile("f1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        OcflAsserts.assertThrowsWithMessage(NotFoundException.class, "The logical path f5 was not found in object o1", () -> {
            repo.fileChangeHistory(objectId, "f5");
        });
    }

    @Test
    public void getObjectFilesFromLazyLoadGetObject() {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        var objectId = "o2";

        var objectVersion = repo.getObject(ObjectVersionId.head(objectId));

        assertThat(objectVersion, objectVersion(objectId, "v3",
                versionInfo(defaultVersionInfo.getUser(), "3"),
                versionFile("dir1/dir2/file2", O2_PATH + "/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("dir1/file3", O2_PATH + "/v3/content/dir1/file3",
                        "This is a different file 3",
                        Map.of(DigestAlgorithm.sha512, "6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331"))
        ));

        objectVersion = repo.getObject(ObjectVersionId.version(objectId, "v1"));

        assertThat(objectVersion, objectVersion(objectId, "v1",
                versionInfo(defaultVersionInfo.getUser(), defaultVersionInfo.getMessage()),
                versionFile("dir1/dir2/file2", O2_PATH + "/v1/content/dir1/dir2/file2",
                        "Test file 2",
                        Map.of(DigestAlgorithm.sha512, "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                versionFile("file1", O2_PATH + "/v1/content/file1",
                        "Test file 1",
                        Map.of(DigestAlgorithm.sha512, "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e"))
        ));
    }

    @Test
    public void acceptEmptyPutObjectRequests() throws IOException {
        var repoName = "repo6";
        var repo = defaultRepo(repoName);

        var objectId = "o4";

        var empty = Files.createDirectory(tempRoot.resolve("empty"));

        repo.putObject(ObjectVersionId.head(objectId), empty, defaultVersionInfo);

        assertEquals(0, repo.getObject(ObjectVersionId.head(objectId)).getFiles().size());
    }

    @Test
    public void removeAllOfTheFilesFromAnObject() throws IOException {
        var repoName = "repo4";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        repo.putObject(ObjectVersionId.head(objectId), sourceObjectPath(objectId, "v1"), defaultVersionInfo);

        var ocflObject = repo.getObject(ObjectVersionId.head(objectId));

        repo.updateObject(ocflObject.getObjectVersionId(), defaultVersionInfo.setMessage("delete content"), updater -> {
            ocflObject.getFiles().forEach(file -> {
                updater.removeFile(file.getPath());
            });
        });

        var outputPath = outputPath(repoName, objectId);
        repo.getObject(ObjectVersionId.head(objectId), outputPath);
        assertEquals(0, outputPath.toFile().list().length);
    }

    @Test
    public void rejectObjectNotFoundWhenObjectDoesNotExists() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectVersionId.head("bogus"), outputPath(repoName, "bogus")));
    }

    @Test
    public void rejectObjectNotFoundWhenObjectExistsButVersionDoesNot() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        var objectId = "o2";

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectVersionId.version(objectId, "v100"), outputPath(repoName, objectId)));
    }

    @Test
    public void shouldUpdateObjectWhenReferenceVersionSpecifiedAndIsMostRecentVersion() {
        var repoName = "repo4";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.version(objectId, "v2"), defaultVersionInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(inputStream(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        });

        verifyRepo(repoName);
    }

    @Test
    public void rejectUpdateObjectWhenReferenceVersionSpecifiedAndIsNotMostRecentVersion() {
        var repoName = "repo4";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        assertThrows(ObjectOutOfSyncException.class, () -> repo.updateObject(ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(inputStream(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        }));
    }

    @Test
    public void shouldCreateNewVersionWhenObjectUpdateWithNoChanges() {
        var repoName = "repo7";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.putObject(ObjectVersionId.head(objectId), sourceObjectPath(objectId, "v1"), defaultVersionInfo.setMessage("1"));
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            // no op
        });

        verifyRepo(repoName);
    }

    @Test
    public void failGetObjectWhenInventoryFixityCheckFails() {
        var repoName = "invalid-inventory-fixity";
        var repoDir = sourceRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        assertThrows(FixityCheckException.class, () -> repo.describeObject("z1"));
    }

    @Test
    public void failToInitRepoWhenObjectsStoredUsingDifferentLayout() {
        var repoName = "repo3";
        var repoDir = expectedRepoPath(repoName);
        assertThrows(IllegalStateException.class, () -> {
            new OcflRepositoryBuilder()
                    .layoutConfig(new HashedTruncatedNTupleConfig().setTupleSize(1))
                    .inventoryMapper(ITestHelper.testInventoryMapper())
                    .storage(FileSystemOcflStorage.builder().repositoryRoot(repoDir).build())
                    .workDir(repoDir.resolve("deposit"))
                    .build();
        });
    }

    @Test
    public void failGetObjectWhenFileFixityCheckFails() {
        var repoName = "invalid-file-fixity";
        var repoDir = sourceRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        assertThrows(FixityCheckException.class, () -> repo.getObject(ObjectVersionId.head("o1"), outputPath(repoName, "blah")));
    }

    @Test
    public void failGetObjectWhenInvalidDigestAlgorithmUsed() {
        var repoName = "invalid-digest-algorithm";
        var repoDir = sourceRepoPath(repoName);
        var repo = existingRepo(repoName, repoDir);

        assertThat(assertThrows(UncheckedIOException.class, () -> {
            repo.getObject(ObjectVersionId.head("o1"));
        }).getMessage(), containsString("digestAlgorithm must be sha512 or sha256"));
    }

    @Test
    public void putObjectWithDuplicateFiles() {
        var repoName = "repo8";
        var repo = defaultRepo(repoName);

        var objectId = "o5";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        // Which duplicate file that's preserved is non-deterministic
        var expectedPaths = ITestHelper.listAllPaths(expectedRepoPath(repoName));
        var actualPaths = listFilesInRepo(repoName);
        assertEquals(expectedPaths.size(), actualPaths.size());
    }

    @Test
    public void useZeroPaddedVersionsWhenExistingVersionIsZeroPadded() {
        var repoName = "zero-padded";
        var repo = existingRepo(repoName, sourceRepoPath(repoName));

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"));

        verifyRepo(repoName);
    }

    @Test
    public void useDifferentContentDirectoryWhenExistingObjectIsUsingDifferentDir() {
        var repoName = "different-content";
        var repo = existingRepo(repoName, sourceRepoPath(repoName));

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"));

        verifyRepo(repoName);
    }

    @Test
    public void shouldUseDifferentDigestAlgorithmWhenInventoryHasDifferent() {
        var repoName = "different-digest";
        var repo = existingRepo(repoName, sourceRepoPath(repoName));

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"));

        verifyRepo(repoName);
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void allowPathsWithDifficultCharsWhenNoRestrictionsApplied() throws IOException {
        var repoName = "repo16";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), null, updater -> {
            updater.writeFile(new ByteArrayInputStream("test1".getBytes()), "backslash\\path\\file");
            updater.writeFile(new ByteArrayInputStream("test3".getBytes()), "fi\u0080le");
        });

        var expectedRepoPath = expectedRepoPath(repoName);
        var backslashFile = expectedRepoPath.resolve(O1_PATH + "/v1/content/backslash\\path\\file");
        try {
            Files.write(backslashFile, "test1".getBytes());
            verifyRepo(repoName);
        } finally {
            Files.deleteIfExists(backslashFile);
        }
    }

    @Test
    public void rejectPathsWhenInvalidAndNotSanitized() {
        var repoName = "repo9";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test2".getBytes()), "file/");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "/absolute/path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "empty//path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "relative/../../path/file");
            });
        });

        assertThrows(PathConstraintException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "./file");
            });
        });
    }

    @Test
    public void reinstateFileThatWasRemoved() {
        var repoName = "repo9";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.removeFile("file3");
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file3", "file3");
        });

        verifyRepo(repoName);
    }

    @Test
    public void reinstateFileThatWasNeverRemoved() {
        var repoName = "repo10";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file2", "file1");
        });

        verifyRepo(repoName);
    }

    @Test
    public void shouldRejectReinstateWhenVersionDoesNotExist() {
        var repoName = "repo10";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertThat(assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
                updater.reinstateFile(VersionId.fromString("v3"), "file2", "file1");
            });
        }).getMessage(), containsString("does not contain a file at"));
    }

    @Test
    public void shouldRejectReinstateWhenFileDoesNotExist() {
        var repoName = "repo10";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
                updater.reinstateFile(VersionId.fromString("v1"), "file4", "file1");
            });
        });
    }

    @Test
    public void purgeObjectWhenExists() {
        var repoName = "purge-object";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.purgeObject(objectId);

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject(objectId);
        });

        assertEquals(2, listFilesInRepo(repoName).size());
    }

    @Test
    public void purgeObjectDoNothingWhenDoesNotExist() {
        var repoName = "purge-object";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.purgeObject("o4");

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject("o4");
        });

        assertEquals(9, listFilesInRepo(repoName).size());
    }

    @Test
    public void shouldCreateNewObjectWithUpdateObjectApi() {
        var repoName = "repo11";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.addPath(sourcePath.resolve("file2"), "file2")
                    .addPath(sourcePath.resolve("file3"), "file3");
        });

        verifyRepo(repoName);
    }

    @Test
    public void shouldReturnObjectExistence() {
        var repoName = "repo11";
        var repo = defaultRepo(repoName);

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectVersionId.head(objectId), sourcePath, defaultVersionInfo.setMessage("1"));

        assertTrue(repo.containsObject(objectId));
        assertFalse(repo.containsObject("o4"));
    }

    @Test
    public void shouldMoveFilesIntoRepoOnPutObjectWhenMoveSourceSpecified() {
        var repoName = "repo3";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo, OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV2, defaultVersionInfo.setMessage("second"), OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectVersionId.head(objectId), sourcePathV3, defaultVersionInfo.setMessage("third"), OcflOption.MOVE_SOURCE);

        verifyRepo(repoName);
        assertFalse(Files.exists(sourcePathV1));
        assertFalse(Files.exists(sourcePathV2));
        assertFalse(Files.exists(sourcePathV3));
    }

    @Test
    public void shouldMoveFilesIntoRepoOnUpdateObjectWhenMoveSourceSpecified() {
        var repoName = "repo4";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectVersionId.head(objectId), sourcePathV1, defaultVersionInfo);

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3", OcflOption.MOVE_SOURCE)
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .addPath(sourcePathV3.resolve("dir1"), "dir1", OcflOption.MOVE_SOURCE);
        });

        verifyRepo(repoName);
        assertEquals(0, ITestHelper.listAllPaths(sourcePathV2).size());
        assertEquals(0, ITestHelper.listAllPaths(sourcePathV3).size());
    }

    @Test
    public void shouldMoveSrcDirContentsIntoSubdirWhenSubdirSpecifiedAsDst() {
        var repoName = "repo12";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.addPath(sourcePathV1, "sub", OcflOption.MOVE_SOURCE);
        });
        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.addPath(sourcePathV2, "sub", OcflOption.MOVE_SOURCE);
        });

        verifyRepo(repoName);
    }

    @Test
    public void shouldAddFileWithFileNameWhenNoDestinationGiven() {
        var repoName = "repo13";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.addPath(sourcePathV1.resolve("file1"));
        });

        var files = repo.describeVersion(ObjectVersionId.head(objectId)).getFiles();

        assertEquals(1, files.size());
        assertEquals("file1", files.iterator().next().getPath());
    }

    @Test
    public void addDirectoryToRootWhenDestinationNotSpecified() {
        var repoName = "repo13";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.addPath(sourcePathV1, "");
        });

        verifyRepo(repoName);
    }

    @Test
    public void writeInputStreamToObjectWhenHasFixityCheckAndValid() {
        var repoName = "repo14";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
            updater.writeFile(
                    new FixityCheckInputStream(inputStream(sourcePath.resolve("file1")), DigestAlgorithm.md5, "95efdf0764d92207b4698025f2518456"),
                    "file1");
        });

        verifyRepo(repoName);
    }

    @Test
    public void failInputStreamToObjectWhenHasFixityCheckAndNotValid() {
        var repoName = "repo14";
        var repo = defaultRepo(repoName);

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        assertThrows(FixityCheckException.class, () -> {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo, updater -> {
                updater.writeFile(
                        new FixityCheckInputStream(inputStream(sourcePath.resolve("file1")), DigestAlgorithm.md5, "bogus"),
                        "file1");
            });
        });
    }

    @Test
    public void replicatePreviousVersionToHead() {
        var repoName = "replicate1";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        repo.replicateVersionAsHead(ObjectVersionId.version(objectId, "v1"), defaultVersionInfo.setMessage("replicate"));

        verifyRepo(repoName);
    }

    @Test
    public void replicatePreviousVersionToHeadWhenHeadVersionSpecified() {
        var repoName = "replicate2";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        repo.replicateVersionAsHead(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("replicate"));

        verifyRepo(repoName);
    }

    @Test
    public void failReplicateWhenVersionDoesNotExist() {
        var repoName = "replicate3";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        OcflAsserts.assertThrowsWithMessage(NotFoundException.class, "version v4 was not found", () -> {
            repo.replicateVersionAsHead(ObjectVersionId.version(objectId, "v4"), defaultVersionInfo.setMessage("replicate"));
        });
    }

    @Test
    public void rollbackToPriorVersion() {
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        repo.rollbackToVersion(ObjectVersionId.version(objectId, "v1"));

        verifyRepo(repoName);
    }

    @Test
    public void doNothingWhenRollbackToHead() {
        var repoName = "rollback2";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        repo.rollbackToVersion(ObjectVersionId.head(objectId));

        verifyRepo(repoName);
    }

    @Test
    public void rollbackToPreviousVersion() {
        var repoName = "rollback3";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        repo.rollbackToVersion(ObjectVersionId.version(objectId, "v2"));

        verifyRepo(repoName);
    }

    @Test
    public void failRollbackWhenVersionDoesNotExist() {
        var repoName = "rollback3";
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

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
            updater.reinstateFile(VersionId.fromString("v1"), "f1", "f1")
                    .writeFile(new ByteArrayInputStream("2.2".getBytes()), "f2", OcflOption.OVERWRITE);
        });

        OcflAsserts.assertThrowsWithMessage(NotFoundException.class, "version v4 was not found", () -> {
            repo.rollbackToVersion(ObjectVersionId.version(objectId, "v4"));
        });
    }

    @Test
    public void useFinerGrainTimestamp() {
        var repoName = "timestamp";
        var repo = defaultRepo(repoName);

        var timestamp = "2020-06-29T13:40:53.703314Z";

        ITestHelper.fixTime(repo, timestamp);

        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        var desc = repo.describeObject(objectId);

        assertEquals(timestamp, desc.getHeadVersion().getCreated().toString());
    }

    @Test
    public void flatLayoutWithValidIds() {
        var repoName = "flat-layout";
        var repo = defaultRepo(repoName, new FlatLayoutConfig());

        var objectId1 = "o1";

        repo.updateObject(ObjectVersionId.head(objectId1), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        var objectId2 = "object-2";

        repo.updateObject(ObjectVersionId.head(objectId2), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        verifyRepo(repoName);
    }

    private void verifyStream(Path expectedFile, OcflObjectVersionFile actual) throws IOException {
        var stream = actual.getStream();
        var contents = TestHelper.inputToString(stream);
        stream.checkFixity();
        assertEquals(TestHelper.inputToString(Files.newInputStream(expectedFile)), contents);
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
