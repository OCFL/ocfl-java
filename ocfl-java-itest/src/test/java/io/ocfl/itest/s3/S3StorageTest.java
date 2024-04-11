package io.ocfl.itest.s3;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import io.ocfl.aws.OcflS3Client;
import io.ocfl.core.storage.cloud.CloudClient;
import io.ocfl.core.storage.cloud.CloudStorage;
import io.ocfl.core.storage.common.Storage;
import io.ocfl.core.util.FileUtil;
import io.ocfl.itest.StorageTest;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.extension.RegisterExtension;
import software.amazon.awssdk.core.async.AsyncRequestBody;
import software.amazon.awssdk.core.async.AsyncResponseTransformer;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

public class S3StorageTest extends StorageTest {

    private static final String REPO_PREFIX =
            "S3StorageTest" + ThreadLocalRandom.current().nextLong();

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private static S3AsyncClient s3Client;
    private static S3AsyncClient tmClient;
    private static S3TransferManager transferManager;
    private static String bucket;

    private Set<String> repoPrefixes = new HashSet<>();

    private String name = "storage-test";

    @BeforeAll
    public static void beforeAll() {
        s3Client = S3ITestHelper.createMockS3Client(S3_MOCK.getServiceEndpoint());
        var tm = S3ITestHelper.createMockTransferManager(S3_MOCK.getServiceEndpoint());
        tmClient = tm.getLeft();
        transferManager = tm.getRight();
        S3StorageTest.bucket = UUID.randomUUID().toString();
        s3Client.createBucket(request -> {
                    request.bucket(S3StorageTest.bucket);
                })
                .join();
    }

    @AfterAll
    public static void afterAll() {
        s3Client.close();
        transferManager.close();
        tmClient.close();
    }

    @AfterEach
    public void after() {
        repoPrefixes.forEach(prefix -> {
            createCloudClient(prefix).deletePath("");
        });
    }

    @Override
    protected Storage newStorage() {
        return new CloudStorage(createCloudClient(name));
    }

    protected void dir(String path) {
        // no op
    }

    protected void file(String path) {
        file(path, "");
    }

    protected void file(String path, String content) {
        s3Client.putObject(
                        request -> {
                            request.bucket(bucket).key(FileUtil.pathJoinFailEmpty(prefix(name), path));
                        },
                        AsyncRequestBody.fromString(content))
                .join();
    }

    protected String readFile(String path) {
        try (var content = s3Client.getObject(
                        request -> {
                            request.bucket(bucket).key(FileUtil.pathJoinFailEmpty(prefix(name), path));
                        },
                        AsyncResponseTransformer.toBlockingInputStream())
                .join()) {
            return new String(content.readAllBytes());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private CloudClient createCloudClient(String name) {
        repoPrefixes.add(name);

        return OcflS3Client.builder()
                .s3Client(s3Client)
                .transferManager(transferManager)
                .bucket(bucket)
                .repoPrefix(prefix(name))
                .build();
    }

    private String prefix(String name) {
        return REPO_PREFIX + "-" + name;
    }
}
