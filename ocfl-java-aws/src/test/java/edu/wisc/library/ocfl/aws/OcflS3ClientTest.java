package edu.wisc.library.ocfl.aws;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.S3Object;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;

public class OcflS3ClientTest {

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private final S3Client awsS3Client = S3_MOCK.createS3ClientV2();

    @TempDir
    public Path tempDir;

    private static final SecureRandom random = new SecureRandom();

    private static final AtomicInteger count = new AtomicInteger(0);

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicPutAndGet(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals("content", client.downloadString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicDownloadFileWhenExists(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");
        client.downloadFile(key, out);

        assertEquals("content", Files.readString(out));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failDownloadFileWhenKeyDoesNotExist(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadFile("bogus", out);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failDownloadStringWhenKeyDoesNotExist(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadString("bogus");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void putWithDigest(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var key = "blah/blah/blah.txt";
        var content = "yawn";

        client.uploadFile(createFile(content), key, md5(content));

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals(content, client.downloadString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void copyObjectWhenExists(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var src = "dir/file1.txt";
        var dst = "file1.txt";
        var content = "something";

        client.uploadFile(createFile(content), src);
        client.copyObject(src, dst);

        assertObjectsExist(bucket, repoPrefix, List.of(src, dst));

        assertEquals(content, client.downloadString(dst));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failCopyWhenSrcDoesNotExist(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        var src = "dir/file1.txt";
        var dst = "file1.txt";
        var content = "something";

        client.uploadFile(createFile(content), src);

        assertThrows(RuntimeException.class, () -> {
            client.copyObject(src + "asdf", dst);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldListKeysInDirectory(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

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

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldListAllKeysUnderPrefix(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

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

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldReturnWhenBucketExists(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        assertTrue(client.bucketExists());

        client = new OcflS3Client(awsS3Client, "bogus", repoPrefix);
        assertFalse(client.bucketExists());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldDeleteAllObjectsUnderPrefix(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        client.deletePath("d1");

        assertObjectsExist(bucket, repoPrefix, List.of("f1", "f2"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldSafeDeleteAllObjectsUnderPrefix(String repoPrefix) {
        var bucket = createBucket();
        var client = new OcflS3Client(awsS3Client, bucket, repoPrefix);

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

    private String createBucket() {
        var bucket = "test-" + count.incrementAndGet();
        awsS3Client.createBucket(CreateBucketRequest.builder().bucket(bucket).build());
        return bucket;
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
        var result = awsS3Client.listObjectsV2(ListObjectsV2Request.builder()
                .bucket(bucket)
                .prefix(repoPrefix)
                .build());

        var actualKeys = result.contents().stream().map(S3Object::key).collect(Collectors.toList());
        var prefixedExpected = expectedKeys.stream().map(k -> FileUtil.pathJoinIgnoreEmpty(repoPrefix, k))
                .collect(Collectors.toList());

        assertThat(actualKeys, containsInAnyOrder(prefixedExpected.toArray(String[]::new)));
    }

}
