package edu.wisc.library.ocfl.core.itest;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.DefaultOcflRepository;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.matcher.OcflMatchers;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.*;
import java.util.function.Supplier;

import static edu.wisc.library.ocfl.core.matcher.OcflMatchers.fileDetails;
import static edu.wisc.library.ocfl.core.matcher.OcflMatchers.versionDetails;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

public class FileSystemOcflITest {

    @TempDir
    public Path tempRoot;

    private Path reposDir;
    private Path outputDir;
    private Path inputDir;

    private CommitInfo defaultCommitInfo;

    @BeforeAll
    public static void beforeAll() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @BeforeEach
    public void setup() throws IOException {
        reposDir = Files.createDirectory(tempRoot.resolve("repos"));
        outputDir = Files.createDirectory(tempRoot.resolve("output"));
        inputDir = Files.createDirectory(tempRoot.resolve("input"));

        defaultCommitInfo = commitInfo("Peter", "peter@example.com", "commit message");
    }

    @Test
    public void putNewObjectAndUpdateMultipleTimesWithAdditionalPuts() {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));
        repo.putObject(ObjectId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        repo.getObject(ObjectId.head(objectId), outputPath1);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o1v3"), objectId, outputPath1);

        repo.getObject(ObjectId.version(objectId, "v2"), outputPath2);
        verifyDirectoryContentsSame(sourcePathV2, outputPath2.getFileName().toString(), outputPath2);

        repo.getObject(ObjectId.version(objectId, "v1"), outputPath3);
        verifyDirectoryContentsSame(sourcePathV1, outputPath3.getFileName().toString(), outputPath3);
    }

    @Test
    public void updateObjectMakeMultipleChangesWithinTheSameVersion() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");
        var outputPath1 = outputPath(repoName, objectId);
        var outputPath2 = outputPath(repoName, objectId + "2");
        var outputPath3 = outputPath(repoName, objectId + "3");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(input(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        repo.getObject(ObjectId.head(objectId), outputPath1);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v3"), objectId, outputPath1);

        repo.getObject(ObjectId.version(objectId, "v2"), outputPath2);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v2"), outputPath2.getFileName().toString(), outputPath2);

        repo.getObject(ObjectId.version(objectId, "v1"), outputPath3);
        verifyDirectoryContentsSame(expectedOutputPath(repoName, "o2v1"), outputPath3.getFileName().toString(), outputPath3);
    }

    @Test
    public void lazyLoadObject() throws IOException {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));
        repo.putObject(ObjectId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);

        var files = repo.lazyLoadObject(ObjectId.head(objectId));
        assertEquals(2, files.size());
        verifyStream(sourcePathV3.resolve("file2"), files.get("file2"));
        verifyStream(sourcePathV3.resolve("file4"), files.get("file4"));

        files = repo.lazyLoadObject(ObjectId.version(objectId, "v2"));
        assertEquals(3, files.size());
        verifyStream(sourcePathV2.resolve("file1"), files.get("file1"));
        verifyStream(sourcePathV2.resolve("file2"), files.get("file2"));
        verifyStream(sourcePathV2.resolve("dir1/file3"), files.get("dir1/file3"));


        files = repo.lazyLoadObject(ObjectId.version(objectId, "v1"));
        assertEquals(2, files.size());
        verifyStream(sourcePathV1.resolve("file1"), files.get("file1"));
        verifyStream(sourcePathV1.resolve("file2"), files.get("file2"));
    }

    @Test
    public void failRenameAndRemoveFilesAddedInTheCurrentVersion() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(UnsupportedOperationException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.addPath(sourcePathV2.resolve("file2"), "dir1/file2")
                        .addPath(sourcePathV2.resolve("file3"), "file3")
                        .renameFile("dir1/file2", "dir2/file3");
            });
        });

        assertThrows(UnsupportedOperationException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.addPath(sourcePathV2.resolve("file2"), "dir1/file2")
                        .removeFile("dir1/file2");
            });
        });
    }

    @Test
    public void describeObject() {
        var repoName = "repo5";
        var repoDir = newRepoDir(repoName);
        var repo = new OcflRepositoryBuilder().prettyPrintJson()
                .fixityAlgorithms(Set.of(DigestAlgorithm.md5))
                .build(new FileSystemOcflStorage(repoDir, new ObjectIdPathMapperBuilder().buildFlatMapper()), repoDir.resolve("deposit"));
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        repo.putObject(ObjectId.head(objectId), sourceObjectPath(objectId, "v1"), defaultCommitInfo.setMessage("1"));
        repo.putObject(ObjectId.head(objectId), sourceObjectPath(objectId, "v2"), defaultCommitInfo.setMessage("2"));
        repo.putObject(ObjectId.head(objectId), sourceObjectPath(objectId, "v3"), defaultCommitInfo.setMessage("3"));

        var objectDetails = repo.describeObject(objectId);

        assertEquals(objectId, objectDetails.getId());
        assertEquals("v3", objectDetails.getHeadVersionId());
        assertEquals(3, objectDetails.getVersions().size());

        assertThat(objectDetails.getVersions().get("v1"), versionDetails(objectId, "v1",
                OcflMatchers.commitInfo(defaultCommitInfo.getUser(), "1"),
                fileDetails("file1", "o1/v1/content/file1", Map.of(
                        "sha512", "96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e",
                        "md5", "95efdf0764d92207b4698025f2518456")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        "sha512", "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        "md5", "55c1824fcae2b1b51cef5037405fc1ad"))
        ));

        assertThat(objectDetails.getVersions().get("v2"), versionDetails(objectId, "v2",
                OcflMatchers.commitInfo(defaultCommitInfo.getUser(), "2"),
                fileDetails("file1", "o1/v2/content/file1", Map.of(
                        "sha512", "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        "md5", "a0a8bfbf51b81caf7aa5be00f5e26669")),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        "sha512", "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        "md5", "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("dir1/file3", "o1/v2/content/dir1/file3", Map.of(
                        "sha512", "cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2",
                        "md5", "72b6193fe19ec99c692eba5c798e6bdf"))
        ));

        assertThat(objectDetails.getVersions().get("v3"), versionDetails(objectId, "v3",
                OcflMatchers.commitInfo(defaultCommitInfo.getUser(), "3"),
                fileDetails("file2", "o1/v1/content/file2", Map.of(
                        "sha512", "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6",
                        "md5", "55c1824fcae2b1b51cef5037405fc1ad")),
                fileDetails("file4", "o1/v2/content/file1", Map.of(
                        "sha512", "aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc",
                        "md5", "a0a8bfbf51b81caf7aa5be00f5e26669"))
        ));

        assertSame(objectDetails.getHeadVersion(), objectDetails.getVersions().get("v3"));
    }

    @Test
    public void readObjectFiles() {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        repo.readObject(ObjectId.head(objectId), reader -> {
            assertThat(reader.describeVersion(), versionDetails(objectId, "v3",
                    OcflMatchers.commitInfo(defaultCommitInfo.getUser(), "3"),
                    fileDetails("dir1/dir2/file2", "o2/v1/content/dir1/dir2/file2",
                            Map.of("sha512", "4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6")),
                    fileDetails("dir1/file3", "o2/v3/content/dir1/file3",
                            Map.of("sha512", "6e027f3dc89e0bfd97e4c2ec6919a8fb793bdc7b5c513bea618f174beec32a66d2fc0ce19439751e2f01ae49f78c56dcfc7b49c167a751c823d09da8419a4331"))));

            var files = reader.listFiles();
            assertThat(files, containsInAnyOrder("dir1/dir2/file2", "dir1/file3"));

            var in = reader.getFile("dir1/file3");
            assertEquals("This is a different file 3", inputToString(in));
        });

        repo.readObject(ObjectId.version(objectId, "v1"), reader -> {
            assertEquals("v1", reader.describeVersion().getVersionId());

            var files = reader.listFiles();
            assertThat(files, containsInAnyOrder("dir1/dir2/file2", "file1"));

            var out = outputPath(repoName, "read").resolve("file1");
            reader.getFile("file1", out);
            assertEquals("Test file 1", fileToString(out));
        });
    }

    @Test
    public void putObjectWithNoFiles() throws IOException {
        var repoName = "repo6";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o4";

        var empty = Files.createDirectory(tempRoot.resolve("empty"));

        repo.putObject(ObjectId.head(objectId), empty, defaultCommitInfo);

        var details = repo.describeObject(objectId);

        assertEquals(1, details.getVersions().size());
        assertEquals(0, details.getHeadVersion().getFiles().size());

        var outputPath = outputPath(repoName, objectId);

        repo.getObject(ObjectId.head(objectId), outputPath);

        assertEquals(0, outputPath.toFile().list().length);
    }

    @Test
    public void removeAllOfTheFilesFromAnObject() throws IOException {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        repo.putObject(ObjectId.head(objectId), sourceObjectPath(objectId, "v1"), defaultCommitInfo);

        repo.readObject(ObjectId.head(objectId), reader -> {
            repo.updateObject(reader.describeVersion().getObjectVersionId(), defaultCommitInfo.setMessage("delete content"), updater -> {
                reader.listFiles().forEach(updater::removeFile);
            });
        });

        var outputPath = outputPath(repoName, objectId);
        repo.getObject(ObjectId.head(objectId), outputPath);
        assertEquals(0, outputPath.toFile().list().length);
    }

    @Test
    public void rejectInvalidObjectIds() throws IOException {
        var repoName = "repo6";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var empty = Files.createDirectory(tempRoot.resolve("empty"));

        assertThrows(IllegalArgumentException.class, () -> repo.putObject(ObjectId.head(".."), empty, defaultCommitInfo));
    }

    @Test
    public void rejectObjectNotFoundWhenObjectDoesNotExists() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectId.head("bogus"), outputPath(repoName, "bogus")));
    }

    @Test
    public void rejectObjectNotFoundWhenObjectExistsButVersionDoesNot() throws IOException {
        var repoName = "repo4";
        var repoDir = expectedRepoPath(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        assertThrows(NotFoundException.class, () -> repo.getObject(ObjectId.version(objectId, "v100"), outputPath(repoName, objectId)));
    }

    @Test
    public void shouldUpdateObjectWhenReferenceVersionSpecifiedAndIsMostRecentVersion() {
        var repoName = "repo4";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectId.version(objectId, "v1"), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectId.version(objectId, "v2"), defaultCommitInfo.setMessage("3"), updater -> {
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
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");
        var sourcePathV2 = sourceObjectPath(objectId, "v2");
        var sourcePathV3 = sourceObjectPath(objectId, "v3");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectId.version(objectId, "v1"), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3")
                    .renameFile("file1", "dir3/file1");
        });

        assertThrows(ObjectOutOfSyncException.class, () -> repo.updateObject(ObjectId.version(objectId, "v1"), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .writeFile(input(sourcePathV3.resolve("dir1/file3")), "dir1/file3");
        }));
    }

    @Test
    public void shouldCreateNewVersionWhenObjectUpdateWithNoChanges() {
        var repoName = "repo7";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        repo.putObject(ObjectId.head(objectId), sourceObjectPath(objectId, "v1"), defaultCommitInfo.setMessage("1"));
        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            // no op
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void failGetObjectWhenInventoryFixityCheckFails() {
        var repoName = "invalid-inventory-fixity";
        var repoDir = sourceRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        assertThrows(FixityCheckException.class, () -> repo.getObject(ObjectId.head("z1"), outputPath(repoName, "blah")));
    }

    @Test
    public void failToInitRepoWhenObjectsStoredUsingDifferentLayout() {
        var repoName = "repo3";
        var repoDir = expectedRepoPath(repoName);
        assertThrows(IllegalStateException.class, () -> {
            new OcflRepositoryBuilder().prettyPrintJson().build(
                    new FileSystemOcflStorage(repoDir, new ObjectIdPathMapperBuilder().buildDefaultPairTreeMapper()),
                    repoDir.resolve("deposit"));
        });
    }

    @Test
    public void failGetObjectWhenFileFixityCheckFails() {
        var repoName = "invalid-file-fixity";
        var repoDir = sourceRepoPath(repoName);
        var repo = defaultRepo(repoDir);

        assertThrows(FixityCheckException.class, () -> repo.getObject(ObjectId.head("o1"), outputPath(repoName, "blah")));
    }

    @Test
    public void putObjectWithDuplicateFiles() {
        var repoName = "repo8";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o5";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        // Which duplicate file that's preserved is non-deterministic
        var expectedPaths = listAllPaths(expectedRepoPath(repoName));
        var actualPaths = listAllPaths(repoDir);
        assertEquals(expectedPaths.size(), actualPaths.size());
    }

    @Test
    public void useZeroPaddedVersionsWhenExistingVersionIsZeroPadded() {
        var repoName = "zero-padded";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void useDifferentContentDirectoryWhenExistingObjectIsUsingDifferentDir() {
        var repoName = "different-content";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldUseDifferentDigestAlgorithmWhenInventoryHasDifferent() {
        var repoName = "different-digest";
        var repoDir = newRepoDir(repoName);
        copyDir(sourceRepoPath(repoName), repoDir);

        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV2 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"));

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void rejectDstPathsThatAreNotRelativeToObjectRoot() {
        var repoName = "repo9";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "/absolute/path/file3");
            });
        });

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("second"), updater -> {
                updater.writeFile(new ByteArrayInputStream("test".getBytes()), "relative/../../path/file3");
            });
        });
    }

    @Test
    public void reinstateFileThatWasRemoved() {
        var repoName = "repo9";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.removeFile("file3");
        });
        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.reinstateFile("v1", "file3", "file3");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void reinstateFileThatWasNeverRemoved() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);
        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.reinstateFile("v1", "file2", "file1");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void shouldRejectReinstateWhenVersionDoesNotExist() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(IllegalStateException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.reinstateFile("v3", "file2", "file1");
            });
        });
    }

    @Test
    public void shouldRejectReinstateWhenFileDoesNotExist() {
        var repoName = "repo10";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
                updater.reinstateFile("v1", "file4", "file1");
            });
        });
    }

    @Test
    public void purgeObjectWhenExists() {
        var repoName = "purge-object";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.purgeObject(objectId);

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject(objectId);
        });

        assertEquals(4, listAllPaths(repoDir).size());
    }

    @Test
    public void purgeObjectDoNothingWhenDoesNotExist() {
        var repoName = "purge-object";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePathV1 = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.purgeObject("o4");

        assertThrows(NotFoundException.class, () -> {
            repo.describeObject("o4");
        });

        assertEquals(14, listAllPaths(repoDir).size());
    }

    @Test
    public void shouldCreateNewObjectWithUpdateObjectApi() {
        var repoName = "repo11";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("1"), updater -> {
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
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o3";

        var sourcePath = sourceObjectPath(objectId, "v2");

        repo.putObject(ObjectId.head(objectId), sourcePath, defaultCommitInfo.setMessage("1"));

        assertTrue(repo.containsObject(objectId));
        assertFalse(repo.containsObject("o4"));
    }

    @Test
    public void shouldMoveFilesIntoRepoOnPutObjectWhenMoveSourceSpecified() {
        var repoName = "repo3";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o1";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo, OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectId.head(objectId), sourcePathV2, defaultCommitInfo.setMessage("second"), OcflOption.MOVE_SOURCE);
        repo.putObject(ObjectId.head(objectId), sourcePathV3, defaultCommitInfo.setMessage("third"), OcflOption.MOVE_SOURCE);

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
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));
        var sourcePathV3 = copyDir(sourceObjectPath(objectId, "v3"), inputDir.resolve("v3"));

        repo.putObject(ObjectId.head(objectId), sourcePathV1, defaultCommitInfo);

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("2"), updater -> {
            updater.addPath(sourcePathV2.resolve("dir1/file3"), "dir1/file3", OcflOption.MOVE_SOURCE)
                    .renameFile("file1", "dir3/file1");
        });

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo.setMessage("3"), updater -> {
            updater.removeFile("dir1/file3").removeFile("dir3/file1")
                    .addPath(sourcePathV3.resolve("dir1"), "dir1", OcflOption.MOVE_SOURCE);
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
        assertEquals(2, listAllPaths(sourcePathV2).size());
        assertEquals(1, listAllPaths(sourcePathV3).size());
    }

    @Test
    public void shouldMoveSrcDirContentsIntoSubdirWhenSubdirSpecifiedAsDst() {
        var repoName = "repo12";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = copyDir(sourceObjectPath(objectId, "v1"), inputDir.resolve("v1"));
        var sourcePathV2 = copyDir(sourceObjectPath(objectId, "v2"), inputDir.resolve("v2"));

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV1, "sub", OcflOption.MOVE_SOURCE);
        });
        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV2, "sub", OcflOption.MOVE_SOURCE);
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void failWhenAddingAFilePathWithNoDestination() {
        var repoName = "repo13";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        assertThrows(IllegalArgumentException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
                updater.addPath(sourcePathV1.resolve("file1"), "");
            });
        });
    }

    @Test
    public void addDirectoryToRootWhenDestinationNotSpecified() {
        var repoName = "repo13";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePathV1 = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
            updater.addPath(sourcePathV1, "");
        });

        verifyDirectoryContentsSame(expectedRepoPath(repoName), repoDir);
    }

    @Test
    public void writeInputStreamToObjectWhenHasFixityCheckAndValid() {
        var repoName = "repo14";
        var repoDir = newRepoDir(repoName);
        var repo = defaultRepo(repoDir);
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
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
        fixTime(repo, "2019-08-05T15:57:53.703314Z");

        var objectId = "o2";

        var sourcePath = sourceObjectPath(objectId, "v1");

        assertThrows(FixityCheckException.class, () -> {
            repo.updateObject(ObjectId.head(objectId), defaultCommitInfo, updater -> {
                updater.writeFile(
                        new FixityCheckInputStream(input(sourcePath.resolve("file1")), DigestAlgorithm.md5.getJavaStandardName(), "bogus"),
                        "file1");
            });
        });
    }

    private void verifyDirectoryContentsSame(Path expected, Path actual) {
        verifyDirectoryContentsSame(expected, expected.getFileName().toString(), actual);
    }

    private void verifyDirectoryContentsSame(Path expected, String expectDirName, Path actual) {
        assertTrue(Files.exists(actual), actual + " should exist");
        assertTrue(Files.isDirectory(actual), actual + "should be a directory");

        assertEquals(expectDirName, actual.getFileName().toString());

        var expectedPaths = listAllPaths(expected);
        var actualPaths = listAllPaths(actual);

        assertEquals(expectedPaths.size(), actualPaths.size(),
                comparingMessage(expected, actual));

        for (int i = 0; i < expectedPaths.size(); i++) {
            var expectedPath = expectedPaths.get(i);
            var actualPath = actualPaths.get(i);

            assertEquals(expected.relativize(expectedPath).toString(), actual.relativize(actualPath).toString());

            if (Files.isDirectory(expectedPath)) {
                assertTrue(Files.isDirectory(actualPath), actualPath + " should be a directory");
            } else {
                assertTrue(Files.isRegularFile(actualPath), actualPath + " should be a file");
                assertEquals(computeDigest(expectedPath), computeDigest(actualPath),
                        comparingMessage(expectedPath, actualPath, actualPath));
            }
        }
    }

    private void verifyStream(Path expectedFile, OcflFileRetriever actual) throws IOException {
        var stream = actual.retrieveFile();
        var contents = inputToString(stream);
        stream.checkFixity();
        assertEquals(inputToString(Files.newInputStream(expectedFile)), contents);
    }

    private void fixTime(OcflRepository repository, String timestamp) {
        ((DefaultOcflRepository) repository).setClock(Clock.fixed(Instant.parse(timestamp), ZoneOffset.UTC));
    }

    private List<Path> listAllPaths(Path root) {
        var allPaths = new TreeSet<Path>();

        try (var walk = Files.walk(root)) {
            walk.filter(p -> {
                var pStr = p.toString();
                return !(pStr.contains(".keep") || pStr.contains("deposit"));
            }).forEach(allPaths::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new ArrayList<>(allPaths);
    }

    private CommitInfo commitInfo(String name, String address, String message) {
        return new CommitInfo().setMessage(message).setUser(new User().setName(name).setAddress(address));
    }

    private Path outputPath(String repoName, String path) {
        try {
            return Files.createDirectories(outputDir.resolve(Paths.get(repoName, path)));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private Path expectedOutputPath(String repoName, String name) {
        return Paths.get("src/test/resources/expected/output", repoName, name);
    }

    private Path expectedRepoPath(String name) {
        return Paths.get("src/test/resources/expected/repos", name);
    }

    private Path sourceObjectPath(String objectId, String version) {
        return Paths.get("src/test/resources/sources/objects", objectId, version);
    }

    private Path sourceRepoPath(String repo) {
        return Paths.get("src/test/resources/sources/repos", repo);
    }

    private String fileToString(Path file) {
        try (var input = Files.newInputStream(file)) {
            return inputToString(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String inputToString(InputStream inputStream) {
        return new Scanner(inputStream).useDelimiter("\\A").next();
    }

    private Path newRepoDir(String name) {
        try {
            return Files.createDirectory(reposDir.resolve(name));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String computeDigest(Path path) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(MessageDigest.getInstance("blake2s-128"), path.toFile()));
        } catch (IOException | NoSuchAlgorithmException e) {
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

    private String comparingMessage(Object o1, Object o2) {
        return String.format("Comparing %s and %s", o1, o2);
    }

    private Supplier<String> comparingMessage(Object o1, Object o2, Path actualPath) {
        return () -> String.format("Comparing %s and %s:\n\n%s", o1, o2, fileToString(actualPath));
    }

    private OcflRepository defaultRepo(Path repoDir) {
        return new OcflRepositoryBuilder().prettyPrintJson().build(
                new FileSystemOcflStorage(repoDir, new ObjectIdPathMapperBuilder().buildFlatMapper()),
                repoDir.resolve("deposit"));
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
