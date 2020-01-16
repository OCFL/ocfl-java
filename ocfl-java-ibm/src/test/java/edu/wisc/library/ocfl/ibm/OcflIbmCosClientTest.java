package edu.wisc.library.ocfl.ibm;

import com.ibm.cloud.objectstorage.auth.AWSStaticCredentialsProvider;
import com.ibm.cloud.objectstorage.auth.BasicAWSCredentials;
import com.ibm.cloud.objectstorage.client.builder.AwsClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3;
import com.ibm.cloud.objectstorage.services.s3.AmazonS3ClientBuilder;
import com.ibm.cloud.objectstorage.services.s3.model.DeleteObjectsRequest;
import com.ibm.cloud.objectstorage.services.s3.model.S3ObjectSummary;
import com.ibm.cloud.objectstorage.services.s3.transfer.TransferManager;
import com.ibm.cloud.objectstorage.services.s3.transfer.TransferManagerBuilder;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Requires the following env variables to be set:
 *
 * OCFL_IBM_COS_TEST_BUCKET
 * OCFL_IBM_COS_TEST_ENDPOINT
 * OCFL_IBM_COS_TEST_ACCESS_KEY
 * OCFL_IBM_COS_TEST_SECRET_KEY
 *
 */
public class OcflIbmCosClientTest {

    private static final String SECRET_KEY = "OCFL_IBM_COS_TEST_SECRET_KEY";

    @TempDir
    public Path tempDir;

    private static final SecureRandom random = new SecureRandom();
    private static final AtomicInteger count = new AtomicInteger(0);

    private static AmazonS3 cosClient;
    private static TransferManager transferManager;
    private static String bucket;

    private String repoPrefix;
    private IbmCosClient client;

    @BeforeAll
    public static void beforeAll() {
        bucket = System.getenv("OCFL_IBM_COS_TEST_BUCKET");

        var endpoint = System.getenv("OCFL_IBM_COS_TEST_ENDPOINT");
        var accessKey = System.getenv("OCFL_IBM_COS_TEST_ACCESS_KEY");
        var secretKey = System.getenv(SECRET_KEY);

        if (bucket != null && endpoint != null && accessKey != null && secretKey != null) {
            cosClient = AmazonS3ClientBuilder.standard()
                    .withCredentials(new AWSStaticCredentialsProvider(new BasicAWSCredentials(accessKey, secretKey)))
                    .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endpoint, "us"))
                    .enablePathStyleAccess()
                    .build();

            transferManager = TransferManagerBuilder.standard().withS3Client(cosClient).build();
        }
    }

    @BeforeEach
    public void setup() {
        if (cosClient != null) {
            repoPrefix = createRepoPrefix();
            client = new IbmCosClient(cosClient, transferManager, bucket, repoPrefix);
        }
    }

    @AfterEach
    public void after() {
        if (client != null) {
            try {
                deleteRepoPrefix();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void basicPutAndGet() {
        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals("content", client.downloadString(key));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void basicDownloadFileWhenExists() throws IOException {
        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");
        client.downloadFile(key, out);

        assertEquals("content", Files.readString(out));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void failDownloadFileWhenKeyDoesNotExist() throws IOException {
        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadFile("bogus", out);
        });
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void failDownloadStringWhenKeyDoesNotExist() throws IOException {
        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadString("bogus");
        });
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void putWithDigest() {
        var key = "blah/blah/blah.txt";
        var content = "yawn";

        client.uploadFile(createFile(content), key, md5(content));

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals(content, client.downloadString(key));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void copyObjectWhenExists() {
        var src = "dir/file1.txt";
        var dst = "file1.txt";
        var content = "something";

        client.uploadFile(createFile(content), src);
        client.copyObject(src, dst);

        assertObjectsExist(bucket, repoPrefix, List.of(src, dst));

        assertEquals(content, client.downloadString(dst));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void failCopyWhenSrcDoesNotExist() {
        var src = "dir/file1.txt";
        var dst = "file1.txt";
        var content = "something";

        client.uploadFile(createFile(content), src);

        assertThrows(RuntimeException.class, () -> {
            client.copyObject(src + "asdf", dst);
        });
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void shouldListKeysInDirectory() {
        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        var result = client.listDirectory("d1");

        assertEquals(2, result.getObjects().size());
        assertObjectListingDir(repoPrefix, "d1/f3", result.getObjects().get(0));
        assertObjectListingDir(repoPrefix, "d1/f4", result.getObjects().get(1));

        assertEquals(1, result.getDirectories().size());
        assertEquals("d1/d2/", result.getDirectories().get(0).getPath());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void shouldListAllKeysUnderPrefix() {
        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        var result = client.list("d1");

        assertEquals(4, result.getObjects().size());
        assertObjectListingAll(repoPrefix, "d1", "d1/d2/f5", result.getObjects().get(0));
        assertObjectListingAll(repoPrefix, "d1", "d1/d2/f6", result.getObjects().get(1));
        assertObjectListingAll(repoPrefix, "d1", "d1/f3", result.getObjects().get(2));
        assertObjectListingAll(repoPrefix, "d1", "d1/f4", result.getObjects().get(3));

        assertEquals(0, result.getDirectories().size());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void shouldReturnWhenBucketExists() {
        assertTrue(client.bucketExists());

        client = new IbmCosClient(cosClient, transferManager, "bogus", repoPrefix);
        assertFalse(client.bucketExists());
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void shouldDeleteAllObjectsUnderPrefix() {
        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        client.deletePath("d1");

        assertObjectsExist(bucket, repoPrefix, List.of("f1", "f2"));
    }

    @Test
    @EnabledIfEnvironmentVariable(named = SECRET_KEY, matches = ".+")
    public void shouldSafeDeleteAllObjectsUnderPrefix() {
        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        client.safeDeleteObjects("d1/f3", "d1/d2/f6");

        assertObjectsExist(bucket, repoPrefix, List.of("f1", "f2", "d1/f4", "d1/d2/f5"));
    }

    private Path createFile(String content) {
        try {
            return Files.writeString(tempDir.resolve("temp-file-" + random.nextLong()), content);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private byte[] bytes(String content) {
        return content.getBytes(StandardCharsets.UTF_8);
    }

    private String createRepoPrefix() {
        return "test-" + OffsetDateTime.now().toEpochSecond() + "-" + count.incrementAndGet() ;
    }

    private void deleteRepoPrefix() {
        var result = cosClient.listObjectsV2(bucket, repoPrefix);
        if (!result.getObjectSummaries().isEmpty()) {
            var keys = result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
            cosClient.deleteObjects(new DeleteObjectsRequest(bucket).withKeys(keys.toArray(String[]::new)));
        }
    }

    private byte[] md5(String string) {
        return DigestUtil.computeDigest(DigestAlgorithm.md5, ByteBuffer.wrap(string.getBytes(StandardCharsets.UTF_8)));
    }

    private void assertObjectListingDir(String repoPrefix, String key, ListResult.ObjectListing actual) {
        assertEquals(key, actual.getKey().getPath());
        assertEquals(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key), actual.getKey().getKey());
        assertEquals(key.substring(key.lastIndexOf('/') + 1), actual.getKeySuffix());
    }

    private void assertObjectListingAll(String repoPrefix, String searchPrefix, String key, ListResult.ObjectListing actual) {
        assertEquals(key, actual.getKey().getPath());
        assertEquals(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key), actual.getKey().getKey());
        assertEquals(key.substring(searchPrefix.length()), actual.getKeySuffix());
    }

    private void assertObjectsExist(String bucket, String repoPrefix, Collection<String> expectedKeys) {
        var result = cosClient.listObjectsV2(bucket, repoPrefix);

        var actualKeys = result.getObjectSummaries().stream().map(S3ObjectSummary::getKey).collect(Collectors.toList());
        var prefixedExpected = expectedKeys.stream().map(k -> FileUtil.pathJoinIgnoreEmpty(repoPrefix, k))
                .collect(Collectors.toList());

        assertThat(actualKeys, containsInAnyOrder(prefixedExpected.toArray(String[]::new)));
    }

}
