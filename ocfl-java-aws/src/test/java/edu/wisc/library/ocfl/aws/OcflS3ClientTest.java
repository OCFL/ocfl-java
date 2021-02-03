package edu.wisc.library.ocfl.aws;

import at.favre.lib.bytes.Bytes;
import com.adobe.testing.s3mock.junit5.S3MockExtension;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.storage.cloud.KeyNotFoundException;
import edu.wisc.library.ocfl.core.storage.cloud.ListResult;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListObjectsV2Request;
import software.amazon.awssdk.services.s3.model.ObjectLockLegalHoldStatus;
import software.amazon.awssdk.services.s3.model.S3Object;
import software.amazon.awssdk.services.s3.model.Tag;
import software.amazon.awssdk.services.s3.model.Tagging;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals("content", client.downloadString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void putObjectWithModification(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = OcflS3Client.builder()
                .s3Client(awsS3Client)
                .bucket(bucket)
                .repoPrefix(repoPrefix)
                .putObjectModifier((key, builder) -> {
                    if (key.endsWith("/test.txt")) {
                        builder.contentType("text/plain");
                    }
                })
                .build();

        var key1 = "dir/sub/test.txt";
        var key2 = "dir/sub/test.json";

        client.uploadFile(createFile("content"), key1);
        client.uploadFile(createFile("content2"), key2);

        assertObjectsExist(bucket, repoPrefix, List.of(key1, key2));

        try (var response = awsS3Client.getObject(builder -> {
            builder.bucket(bucket).key(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key1)).build();
        })) {
            assertEquals("text/plain", response.response().contentType());
        }
        try (var response = awsS3Client.getObject(builder -> {
            builder.bucket(bucket).key(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key2)).build();
        })) {
            assertEquals("application/octet-stream", response.response().contentType());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void multipartUpload(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);
        client.setMaxPartBytes(1024 * 1024);
        client.setPartSizeBytes(1024 * 100);

        var key = "dir/sub/test.txt";

        var byteString = Bytes.random(1024 * 1024 + 100).encodeHex();

        client.uploadFile(createFile(byteString), key);

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals(byteString, client.downloadString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void multipartUploadWithModification(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = OcflS3Client.builder()
                .s3Client(awsS3Client)
                .bucket(bucket)
                .repoPrefix(repoPrefix)
                .putObjectModifier((key, builder) -> {
                    if (key.endsWith("/test.txt")) {
                        builder.contentType("text/plain");
                    }
                })
                .build();
        client.setMaxPartBytes(1024 * 1024);
        client.setPartSizeBytes(1024 * 100);

        var key1 = "dir/sub/test.txt";
        var key2 = "dir/sub/test.json";

        var byteString1 = Bytes.random(1024 * 1024 + 100).encodeHex();
        var byteString2 = Bytes.random(1024 * 1024 + 100).encodeHex();

        client.uploadFile(createFile(byteString1), key1);
        client.uploadFile(createFile(byteString2), key2);

        assertObjectsExist(bucket, repoPrefix, List.of(key1, key2));

        try (var response = awsS3Client.getObject(builder -> {
            builder.bucket(bucket).key(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key1)).build();
        })) {
            assertEquals("text/plain", response.response().contentType());
        }
        try (var response = awsS3Client.getObject(builder -> {
            builder.bucket(bucket).key(FileUtil.pathJoinIgnoreEmpty(repoPrefix, key2)).build();
        })) {
            assertEquals("application/octet-stream", response.response().contentType());
        }
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void basicDownloadFileWhenExists(String repoPrefix) throws IOException {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");
        client.downloadFile(key, out);

        assertEquals("content", Files.readString(out));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failDownloadFileWhenKeyDoesNotExist(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var out = tempDir.resolve("test.txt");

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadFile("bogus", out);
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failDownloadStringWhenKeyDoesNotExist(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertThrows(KeyNotFoundException.class, () -> {
            client.downloadString("bogus");
        });
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void putWithContentType(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "blah/blah/blah.txt";
        var content = "yawn";

        client.uploadFile(createFile(content), key, "text/plain; charset=utf-8");

        assertObjectsExist(bucket, repoPrefix, List.of(key));

        assertEquals(content, client.downloadString(key));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void copyObjectWhenExists(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

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
        var client = createClient(bucket, repoPrefix);

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
        var client = createClient(bucket, repoPrefix);

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
        assertEquals("d2", result.getDirectories().get(0).getName());

        result = client.listDirectory("");

        assertEquals(2, result.getObjects().size());
        assertObjectListingDir(repoPrefix, "f1", result.getObjects().get(0));
        assertObjectListingDir(repoPrefix, "f2", result.getObjects().get(1));

        assertEquals(1, result.getDirectories().size());
        assertEquals("d1/", result.getDirectories().get(0).getPath());
        assertEquals("d1", result.getDirectories().get(0).getName());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldListAllKeysUnderPrefix(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

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
        var client = createClient(bucket, repoPrefix);

        assertTrue(client.bucketExists());

        client = createClient("bogus", repoPrefix);
        assertFalse(client.bucketExists());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void shouldDeleteAllObjectsUnderPrefix(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

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
        var client = createClient(bucket, repoPrefix);

        client.uploadBytes("f1", bytes("1"), null);
        client.uploadBytes("f2", bytes("2"), null);
        client.uploadBytes("d1/f3", bytes("3"), null);
        client.uploadBytes("d1/f4", bytes("4"), null);
        client.uploadBytes("d1/d2/f5", bytes("5"), null);
        client.uploadBytes("d1/d2/f6", bytes("6"), null);

        client.safeDeleteObjects("d1/f3", "d1/d2/f6");

        assertObjectsExist(bucket, repoPrefix, List.of("f1", "f2", "d1/f4", "d1/d2/f5"));
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void headWhenExists(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        var result = client.head(key);

        assertEquals(7, result.getContentLength());
        assertEquals("\"9a0364b9e99bb480dd25e1f0284c8555\"", result.getETag());
        assertNotNull(result.getLastModified());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "ocfl-repo-1"})
    public void failHeadWhenDoesNotExist(String repoPrefix) {
        var bucket = createBucket();
        var client = createClient(bucket, repoPrefix);

        var key = "dir/sub/test.txt";

        client.uploadFile(createFile("content"), key);

        assertThrows(KeyNotFoundException.class, () -> {
            client.head("bogus");
        });
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

    private OcflS3Client createClient(String bucket, String repoPrefix) {
        return OcflS3Client.builder()
                .s3Client(awsS3Client)
                .bucket(bucket)
                .repoPrefix(repoPrefix)
                .build();
    }

}
