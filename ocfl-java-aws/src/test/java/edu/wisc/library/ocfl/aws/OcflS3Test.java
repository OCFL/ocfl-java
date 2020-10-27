package edu.wisc.library.ocfl.aws;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.core.OcflRepositoryBuilder;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
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

    private static final String O1_PATH = "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc/";

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

        repo.stageChanges(ObjectVersionId.head(objectId), versionInfo("initial commit", "Peter", "winckles@wisc.edu"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        assertObjectsExist(bucket, repoPrefix, O1_PATH, List.of(
                "0=ocfl_object_1.0",
                "inventory.json",
                "inventory.json.sha512",
                "v1/inventory.json",
                "v1/inventory.json.sha512",
                "extensions/0004-mutable-head/root-inventory.json.sha512",
                "extensions/0004-mutable-head/revisions/r1",
                "extensions/0004-mutable-head/head/inventory.json",
                "extensions/0004-mutable-head/head/inventory.json.sha512",
                "extensions/0004-mutable-head/head/content/r1/dir/file1.txt",
                "extensions/0004-mutable-head/head/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file1", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/file1.txt").getStream()));

        repo.commitStagedChanges(objectId, versionInfo("commit", "Peter", "winckles@wisc.edu"));

        assertObjectsExist(bucket, repoPrefix, O1_PATH, List.of(
                "0=ocfl_object_1.0",
                "inventory.json",
                "inventory.json.sha512",
                "v1/inventory.json",
                "v1/inventory.json.sha512",
                "v2/inventory.json",
                "v2/inventory.json.sha512",
                "v2/content/r1/dir/file1.txt",
                "v2/content/r1/dir/sub/file2.txt"
        ));

        assertEquals("file2", streamToString(repo.getObject(ObjectVersionId.head(objectId)).getFile("dir/sub/file2.txt").getStream()));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicPutTest(String repoPrefix) {
        var bucket = createBucket();
        var repo = createRepo(s3Client, bucket, repoPrefix);
        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), versionInfo("initial commit", "Peter", "winckles@wisc.edu"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        assertObjectsExist(bucket, repoPrefix, O1_PATH, List.of(
                "0=ocfl_object_1.0",
                "inventory.json",
                "inventory.json.sha512",
                "v1/inventory.json",
                "v1/inventory.json.sha512",
                "v1/content/dir/file1.txt",
                "v1/content/dir/sub/file2.txt"
        ));

        repo.updateObject(ObjectVersionId.head(objectId), versionInfo("initial commit", "Peter", "winckles@wisc.edu"), updater -> {
            updater.writeFile(stream("file3"), "dir/sub/file3.txt", OcflOption.OVERWRITE);
        });

        assertObjectsExist(bucket, repoPrefix, O1_PATH, List.of(
                "0=ocfl_object_1.0",
                "inventory.json",
                "inventory.json.sha512",
                "v1/inventory.json",
                "v1/inventory.json.sha512",
                "v1/content/dir/file1.txt",
                "v1/content/dir/sub/file2.txt",
                "v2/inventory.json",
                "v2/inventory.json.sha512",
                "v2/content/dir/sub/file3.txt"
        ));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicPurgeTest(String repoPrefix) {
        var bucket = createBucket();
        var repo = createRepo(s3Client, bucket, repoPrefix);
        var objectId = "o1";

        repo.updateObject(ObjectVersionId.head(objectId), versionInfo("initial commit", "Peter", "winckles@wisc.edu"), updater -> {
            updater.writeFile(stream("file1"), "dir/file1.txt");
            updater.writeFile(stream("file2"), "dir/sub/file2.txt");
            updater.writeFile(stream("file1"), "dir/sub/file3.txt");
        });

        repo.purgeObject(objectId);

        assertFalse(repo.containsObject(objectId));
        assertObjectsExist(bucket, repoPrefix,O1_PATH, List.of());
    }

    private void assertObjectsExist(String bucket, String repoPrefix, String prefix, Collection<String> expectedKeys) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(FileUtil.pathJoinIgnoreEmpty(repoPrefix, prefix))
                .build());

        var actualKeys = result.contents().stream().map(S3Object::key).collect(Collectors.toList());
        var prefixedExpected = expectedKeys.stream().map(k -> FileUtil.pathJoinIgnoreEmpty(repoPrefix, prefix, k))
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
                .layoutConfig(new HashedTruncatedNTupleConfig())
                .prettyPrintJson()
                .contentPathConstraints(ContentPathConstraints.cloud())
                .storage(CloudOcflStorage.builder()
                        .cloudClient(new OcflS3Client(s3Client, bucket, repoPrefix))
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

    private VersionInfo versionInfo(String message, String name, String address) {
        return new VersionInfo().setMessage(message).setUser(name, address);
    }

}
