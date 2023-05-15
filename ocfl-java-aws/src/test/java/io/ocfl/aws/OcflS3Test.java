package io.ocfl.aws;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.OcflOption;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.path.constraint.ContentPathConstraints;
import io.ocfl.core.storage.cloud.CloudClient;
import io.ocfl.core.util.FileUtil;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.Collectors;

import org.apache.commons.lang3.StringUtils;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

public class OcflS3Test {

    private static final Logger LOG = LoggerFactory.getLogger(OcflS3Test.class);

    private static final String O1_PATH =
            "235/2da/728/2352da7280f1decc3acf1ba84eb945c9fc2b7b541094e1d0992dbffd1b6664cc/";

    private static final String REPO_PREFIX =
            "OcflS3Test" + ThreadLocalRandom.current().nextLong();

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private static S3Client s3Client;
    private static CloudClient cloudClient;
    private static String bucket;

    @TempDir
    public Path tempDir;

    @BeforeAll
    public static void beforeAll() {
        var accessKey = System.getenv().get("OCFL_TEST_AWS_ACCESS_KEY");
        var secretKey = System.getenv().get("OCFL_TEST_AWS_SECRET_KEY");
        var bucket = System.getenv().get("OCFL_TEST_S3_BUCKET");

        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey) && StringUtils.isNotBlank(bucket)) {
            LOG.info("Running tests against AWS");
            s3Client = S3Client.builder()
                    .region(Region.US_EAST_2)
                    .credentialsProvider(
                            StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                    .httpClientBuilder(ApacheHttpClient.builder())
                    .build();
            OcflS3Test.bucket = bucket;
        } else {
            LOG.info("Running tests against S3 Mock");
            s3Client = S3_MOCK.createS3ClientV2();
            OcflS3Test.bucket = UUID.randomUUID().toString();
            s3Client.createBucket(request -> {
                request.bucket(OcflS3Test.bucket);
            });
        }

        cloudClient = OcflS3Client.builder()
                .s3Client(s3Client)
                .bucket(OcflS3Test.bucket)
                .repoPrefix(REPO_PREFIX)
                .build();
    }

    @AfterEach
    public void after() {
        cloudClient.deletePath("");
    }

    @Test
    public void basicMutableHeadTest() {
        var repo = createRepo();
        var objectId = "o1";

        repo.stageChanges(
                ObjectVersionId.head(objectId),
                versionInfo("initial commit", "Peter", "winckles@wisc.edu"),
                updater -> {
                    updater.writeFile(stream("file1"), "dir/file1.txt");
                    updater.writeFile(stream("file2"), "dir/sub/file2.txt");
                    updater.writeFile(stream("file1"), "dir/sub/file3.txt");
                });

        assertObjectsExist(
                bucket,
                O1_PATH,
                List.of(
                        "0=ocfl_object_1.1",
                        "inventory.json",
                        "inventory.json.sha512",
                        "v1/inventory.json",
                        "v1/inventory.json.sha512",
                        "extensions/0005-mutable-head/root-inventory.json.sha512",
                        "extensions/0005-mutable-head/revisions/r1",
                        "extensions/0005-mutable-head/head/inventory.json",
                        "extensions/0005-mutable-head/head/inventory.json.sha512",
                        "extensions/0005-mutable-head/head/content/r1/dir/file1.txt",
                        "extensions/0005-mutable-head/head/content/r1/dir/sub/file2.txt"));

        assertEquals(
                "file1",
                streamToString(repo.getObject(ObjectVersionId.head(objectId))
                        .getFile("dir/file1.txt")
                        .getStream()));

        repo.commitStagedChanges(objectId, versionInfo("commit", "Peter", "winckles@wisc.edu"));

        assertObjectsExist(
                bucket,
                O1_PATH,
                List.of(
                        "0=ocfl_object_1.1",
                        "inventory.json",
                        "inventory.json.sha512",
                        "v1/inventory.json",
                        "v1/inventory.json.sha512",
                        "v2/inventory.json",
                        "v2/inventory.json.sha512",
                        "v2/content/r1/dir/file1.txt",
                        "v2/content/r1/dir/sub/file2.txt"));

        assertEquals(
                "file2",
                streamToString(repo.getObject(ObjectVersionId.head(objectId))
                        .getFile("dir/sub/file2.txt")
                        .getStream()));
    }

    @Test
    public void basicPutTest() {
        var repo = createRepo();
        var objectId = "o1";

        repo.updateObject(
                ObjectVersionId.head(objectId),
                versionInfo("initial commit", "Peter", "winckles@wisc.edu"),
                updater -> {
                    updater.writeFile(stream("file1"), "dir/file1.txt");
                    updater.writeFile(stream("file2"), "dir/sub/file2.txt");
                    updater.writeFile(stream("file1"), "dir/sub/file3.txt");
                });

        assertObjectsExist(
                bucket,
                O1_PATH,
                List.of(
                        "0=ocfl_object_1.1",
                        "inventory.json",
                        "inventory.json.sha512",
                        "v1/inventory.json",
                        "v1/inventory.json.sha512",
                        "v1/content/dir/file1.txt",
                        "v1/content/dir/sub/file2.txt"));

        repo.updateObject(
                ObjectVersionId.head(objectId),
                versionInfo("initial commit", "Peter", "winckles@wisc.edu"),
                updater -> {
                    updater.writeFile(stream("file3"), "dir/sub/file3.txt", OcflOption.OVERWRITE);
                });

        assertObjectsExist(
                bucket,
                O1_PATH,
                List.of(
                        "0=ocfl_object_1.1",
                        "inventory.json",
                        "inventory.json.sha512",
                        "v1/inventory.json",
                        "v1/inventory.json.sha512",
                        "v1/content/dir/file1.txt",
                        "v1/content/dir/sub/file2.txt",
                        "v2/inventory.json",
                        "v2/inventory.json.sha512",
                        "v2/content/dir/sub/file3.txt"));
    }

    @Test
    public void basicPurgeTest() {
        var repo = createRepo();
        var objectId = "o1";

        repo.updateObject(
                ObjectVersionId.head(objectId),
                versionInfo("initial commit", "Peter", "winckles@wisc.edu"),
                updater -> {
                    updater.writeFile(stream("file1"), "dir/file1.txt");
                    updater.writeFile(stream("file2"), "dir/sub/file2.txt");
                    updater.writeFile(stream("file1"), "dir/sub/file3.txt");
                });

        repo.purgeObject(objectId);

        assertFalse(repo.containsObject(objectId));
        assertObjectsExist(bucket, O1_PATH, List.of());
    }

    private void assertObjectsExist(String bucket, String prefix, Collection<String> expectedKeys) {
        var result = s3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(FileUtil.pathJoinIgnoreEmpty(REPO_PREFIX, prefix))
                .build());

        var actualKeys = result.contents().stream().map(S3Object::key).collect(Collectors.toList());
        var prefixedExpected = expectedKeys.stream()
                .map(k -> FileUtil.pathJoinIgnoreEmpty(REPO_PREFIX, prefix, k))
                .collect(Collectors.toList());

        assertThat(actualKeys, containsInAnyOrder(prefixedExpected.toArray(String[]::new)));
    }

    private MutableOcflRepository createRepo() {
        return new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .prettyPrintJson()
                .contentPathConstraints(ContentPathConstraints.cloud())
                .storage(storage -> {
                    storage.cloud(cloudClient);
                })
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
