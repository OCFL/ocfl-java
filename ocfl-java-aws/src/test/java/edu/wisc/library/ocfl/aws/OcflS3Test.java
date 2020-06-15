package edu.wisc.library.ocfl.aws;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.storage.cloud.CloudOcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

public class OcflS3Test {

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private final S3Client s3Client = S3_MOCK.createS3ClientV2();
    
    @TempDir
    public Path tempDir;

    private static final AtomicInteger count = new AtomicInteger(0);

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicMutableHeadTest(String repoPrefix) {
        var bucket = createBucket();
        var repo = createRepo(s3Client, bucket, repoPrefix);
        var objectId = "o1";

        repo.stageChanges(ObjectVersionId.head(objectId), commitInfo("Peter", "winckles@wisc.edu", "initial commit"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        assertObjectsExist(bucket, repoPrefix, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/extensions/0004-mutable-head/root-inventory.json.sha512",
                "o1/extensions/0004-mutable-head/revisions/r1",
                "o1/extensions/0004-mutable-head/head/inventory.json",
                "o1/extensions/0004-mutable-head/head/inventory.json.sha512",
                "o1/extensions/0004-mutable-head/head/content/r1/dir/file1.txt",
                "o1/extensions/0004-mutable-head/head/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file1", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/file1.txt").getStream()));

        repo.commitStagedChanges(objectId, commitInfo("Peter", "winckles@wisc.edu", "commit"));

        assertObjectsExist(bucket, repoPrefix, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/v2/inventory.json",
                "o1/v2/inventory.json.sha512",
                "o1/v2/content/r1/dir/file1.txt",
                "o1/v2/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file2", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/sub/file2.txt").getStream()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicPutTest(String repoPrefix) {
        var bucket = createBucket();
        var repo = createRepo(s3Client, bucket, repoPrefix);
        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), commitInfo("Peter", "winckles@wisc.edu", "initial commit"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        assertObjectsExist(bucket, repoPrefix, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/v1/content/dir/file1.txt",
                "o1/v1/content/dir/sub/file2.txt"
        ));

        repo.updateObject(ObjectVersionId.head(objectId), commitInfo("Peter", "winckles@wisc.edu", "initial commit"), updater -> {
            updater.writeFile(stream("file3"), "dir/sub/file3.txt", OcflOption.OVERWRITE);
        });

        assertObjectsExist(bucket, repoPrefix, "o1/", List.of(
                "o1/0=ocfl_object_1.0",
                "o1/inventory.json",
                "o1/inventory.json.sha512",
                "o1/v1/inventory.json",
                "o1/v1/inventory.json.sha512",
                "o1/v1/content/dir/file1.txt",
                "o1/v1/content/dir/sub/file2.txt",
                "o1/v2/inventory.json",
                "o1/v2/inventory.json.sha512",
                "o1/v2/content/dir/sub/file3.txt"
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicPurgeTest(String repoPrefix) {
        var bucket = createBucket();
        var repo = createRepo(s3Client, bucket, repoPrefix);
        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), commitInfo("Peter", "winckles@wisc.edu", "initial commit"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        repo.purgeObject(objectId);

        assertFalse(repo.containsObject(objectId));
        assertObjectsExist(bucket, repoPrefix,"o1/", List.of());
    }

    private void assertObjectsExist(String bucket, String repoPrefix, String prefix, Collection<String> expectedKeys) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(FileUtil.pathJoinIgnoreEmpty(repoPrefix, prefix))
                .build());

        var actualKeys = result.contents().stream().map(S3Object::key).collect(Collectors.toList());
        var prefixedExpected = expectedKeys.stream().map(k -> FileUtil.pathJoinIgnoreEmpty(repoPrefix, k))
                .collect(Collectors.toList());

        assertThat(actualKeys, containsInAnyOrder(prefixedExpected.toArray(String[]::new)));
    }

    private String createBucket() {
        var bucket = "test-" + count.incrementAndGet();
        s3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        return bucket;
    }

    private MutableOcflRepository createRepo(S3Client s3Client, String bucket, String repoPrefix) {
        return new OcflRepositoryBuilder()
                .layoutConfig(DefaultLayoutConfig.flatUrlConfig())
                .prettyPrintJson()
                .contentPathConstraints(ContentPathConstraints.cloud())
                .storage(CloudOcflStorage.builder()
                        .cloudClient(new OcflS3Client(s3Client, bucket, repoPrefix))
                        .workDir(tempDir)
                        .build())
                .workDir(tempDir)
                .buildMutable();
    }

    private InputStream stream(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }

    private String streamToString(InputStream stream) {
        try {
            return new String(stream.readAllBytes());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private CommitInfo commitInfo(String user, String address, String message) {
        return new CommitInfo().setUser(new User().setName(user).setAddress(address)).setMessage(message);
    }

}
