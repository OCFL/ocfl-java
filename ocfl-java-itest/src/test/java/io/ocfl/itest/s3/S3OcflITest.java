package io.ocfl.itest.s3;

import static io.ocfl.itest.TestHelper.inputStream;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.adobe.testing.s3mock.junit5.S3MockExtension;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import io.ocfl.api.OcflOption;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.exception.OcflInputException;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.aws.OcflS3Client;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.cache.NoOpCache;
import io.ocfl.core.extension.UnsupportedExtensionBehavior;
import io.ocfl.core.extension.storage.layout.config.FlatLayoutConfig;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.path.constraint.ContentPathConstraints;
import io.ocfl.core.path.mapper.LogicalPathMappers;
import io.ocfl.core.storage.cloud.CloudClient;
import io.ocfl.core.util.FileUtil;
import io.ocfl.itest.ITestHelper;
import io.ocfl.itest.OcflITest;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

public class S3OcflITest extends OcflITest {

    private static final Logger LOG = LoggerFactory.getLogger(S3OcflITest.class);

    private static final String ENV_ACCESS_KEY = "OCFL_TEST_AWS_ACCESS_KEY";
    private static final String ENV_SECRET_KEY = "OCFL_TEST_AWS_SECRET_KEY";
    private static final String ENV_BUCKET = "OCFL_TEST_S3_BUCKET";

    private static final String REPO_PREFIX =
            "S3OcflITest" + ThreadLocalRandom.current().nextLong();

    @RegisterExtension
    public static S3MockExtension S3_MOCK = S3MockExtension.builder().silent().build();

    private static S3AsyncClient s3Client;
    private static S3AsyncClient tmClient;
    private static S3TransferManager transferManager;
    private static String bucket;

    private static ComboPooledDataSource dataSource;

    private S3ITestHelper s3Helper;
    private Set<String> repoPrefixes = new HashSet<>();

    @BeforeAll
    public static void beforeAll() {
        var accessKey = System.getenv().get(ENV_ACCESS_KEY);
        var secretKey = System.getenv().get(ENV_SECRET_KEY);
        var bucket = System.getenv().get(ENV_BUCKET);

        if (StringUtils.isNotBlank(accessKey) && StringUtils.isNotBlank(secretKey) && StringUtils.isNotBlank(bucket)) {
            LOG.warn("Running tests against AWS");
            s3Client = S3ITestHelper.createS3Client(accessKey, secretKey);
            var tm = S3ITestHelper.createTransferManager(accessKey, secretKey);
            tmClient = tm.getLeft();
            transferManager = tm.getRight();
            S3OcflITest.bucket = bucket;
        } else {
            LOG.warn("Running tests against S3 Mock");
            s3Client = S3ITestHelper.createMockS3Client(S3_MOCK.getServiceEndpoint());
            var tm = S3ITestHelper.createMockTransferManager(S3_MOCK.getServiceEndpoint());
            tmClient = tm.getLeft();
            transferManager = tm.getRight();
            S3OcflITest.bucket = UUID.randomUUID().toString();
            s3Client.createBucket(request -> {
                        request.bucket(S3OcflITest.bucket);
                    })
                    .join();
        }

        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(System.getProperty("db.url", "jdbc:h2:mem:test"));
        dataSource.setUser(System.getProperty("db.user", ""));
        dataSource.setPassword(System.getProperty("db.password", ""));
    }

    @AfterAll
    public static void afterAll() {
        s3Client.close();
        transferManager.close();
        tmClient.close();
    }

    @Override
    protected void onBefore() {
        s3Helper = new S3ITestHelper(s3Client);
    }

    @Override
    protected void onAfter() {
        repoPrefixes.forEach(prefix -> {
            createCloudClient(prefix).deletePath("");
        });
    }

    // Doesn't work with s3 mock
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    @Test
    public void makeContentPathsWindowsSafe() throws IOException {
        var repoName = "windows-safe";
        var repo = defaultRepo(repoName, builder -> builder.defaultLayoutConfig(new FlatLayoutConfig())
                .logicalPathMapper(LogicalPathMappers.percentEncodingWindowsMapper())
                .contentPathConstraints(ContentPathConstraints.windows()));

        var logicalPath = "tést/<bad>:Path 1/\\|obj/?8*%id/#{something}/[0]/۞.txt";

        repo.updateObject(ObjectVersionId.head("o1"), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), logicalPath);
        });

        verifyRepo(repoName);

        var object = repo.getObject(ObjectVersionId.head("o1"));

        assertTrue(object.containsFile(logicalPath), "expected object to contain logical path " + logicalPath);

        try (var stream = object.getFile(logicalPath).getStream()) {
            assertEquals("1", new String(stream.readAllBytes()));
        }
    }

    // Doesn't work with mock https://github.com/adobe/S3Mock/issues/215
    @Test
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    public void listObjectsInRepo() {
        var repoName = "repo-list";
        var repo = defaultRepo(repoName);

        repo.updateObject(ObjectVersionId.head("o1"), defaultVersionInfo, updater -> {
            updater.writeFile(inputStream("test1"), "test1.txt");
        });
        repo.updateObject(ObjectVersionId.head("o2"), defaultVersionInfo, updater -> {
            updater.writeFile(inputStream("test2"), "test2.txt");
        });
        repo.updateObject(ObjectVersionId.head("o3"), defaultVersionInfo, updater -> {
            updater.writeFile(inputStream("test3"), "test3.txt");
        });

        try (var objectIdsStream = repo.listObjectIds()) {
            var objectIds = objectIdsStream.collect(Collectors.toList());
            assertThat(objectIds, containsInAnyOrder("o1", "o2", "o3"));
        }
    }

    // Doesn't work with mock https://github.com/adobe/S3Mock/issues/215
    @Test
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    public void shouldNotListObjectsWithinTheExtensionsDir() {
        var repoName = "repo-multiple-objects";
        var repoRoot = ITestHelper.sourceRepoPath(repoName);

        var repo = existingRepo(repoName, repoRoot, builder -> {
            builder.unsupportedExtensionBehavior(UnsupportedExtensionBehavior.WARN);
        });

        try (var list = repo.listObjectIds()) {
            assertThat(list.collect(Collectors.toList()), containsInAnyOrder("o1", "o2", "o3"));
        }
    }

    // Doesn't work with mock https://github.com/adobe/S3Mock/issues/215
    @Test
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    public void shouldReturnNoValidationErrorsWhenObjectIsValid() {
        var repoName = "valid-repo";
        var repo = defaultRepo(repoName);

        var objectId = "uri:valid-object";
        var versionInfo =
                new VersionInfo().setUser("Peter", "mailto:peter@example.com").setMessage("message");

        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
            updater.writeFile(ITestHelper.streamString("file1"), "file1");
            updater.writeFile(ITestHelper.streamString("file2"), "file2");
        });

        repo.updateObject(ObjectVersionId.head(objectId), versionInfo, updater -> {
            updater.writeFile(ITestHelper.streamString("file3"), "file3").removeFile("file2");
        });

        var results = repo.validateObject(objectId, true);

        assertEquals(0, results.getErrors().size(), () -> results.getErrors().toString());
        assertEquals(
                0, results.getWarnings().size(), () -> results.getWarnings().toString());
    }

    // This test doesn't work with S3Mock because it double encodes
    @Test
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    public void hashedIdLayoutLongEncoded() {
        var repoName = "hashed-id-layout-2";
        var repo = defaultRepo(
                repoName, builder -> builder.defaultLayoutConfig(new HashedNTupleIdEncapsulationLayoutConfig()));

        var objectId = "۵ݨݯژښڙڜڛڝڠڱݰݣݫۯ۞ۆݰ";

        repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
            updater.writeFile(new ByteArrayInputStream("1".getBytes()), "f1")
                    .writeFile(new ByteArrayInputStream("2".getBytes()), "f2");
        });

        verifyRepo(repoName);
    }

    // There appears to be a bug with s3mock's copy object that makes this test fail for some reason
    @Test
    @EnabledIfEnvironmentVariable(named = ENV_ACCESS_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_SECRET_KEY, matches = ".+")
    @EnabledIfEnvironmentVariable(named = ENV_BUCKET, matches = ".+")
    public void writeToObjectConcurrently() {
        var repoName = "repo18";
        var repo = defaultRepo(repoName);

        var objectId = "o1";

        var executor = Executors.newFixedThreadPool(10);

        try {
            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("1"), updater -> {
                var latch = new CountDownLatch(10);
                var futures = new ArrayList<Future<?>>();

                for (int i = 0; i < 5; i++) {
                    futures.add(executor.submit(() -> {
                        latch.countDown();
                        updater.writeFile(
                                ITestHelper.streamString("file1".repeat(100)), "a/b/c/file1.txt", OcflOption.OVERWRITE);
                    }));
                }

                for (int i = 0; i < 5; i++) {
                    var n = i;
                    futures.add(executor.submit(() -> {
                        latch.countDown();
                        updater.writeFile(
                                ITestHelper.streamString(String.valueOf(n).repeat(100)),
                                String.format("a/b/c/d/%s.txt", n));
                    }));
                }

                joinFutures(futures);
            });

            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("2"), updater -> {
                var latch = new CountDownLatch(10);
                var futures = new ArrayList<Future<?>>();

                var errors = new AtomicInteger();

                for (int i = 0; i < 5; i++) {
                    futures.add(executor.submit(() -> {
                        latch.countDown();
                        try {
                            updater.renameFile("a/b/c/file1.txt", "a/b/c/file2.txt");
                        } catch (OcflInputException e) {
                            errors.getAndIncrement();
                        }
                    }));
                }

                futures.add(executor.submit(() -> {
                    latch.countDown();
                    updater.removeFile("a/b/c/d/0.txt");
                }));
                futures.add(executor.submit(() -> {
                    latch.countDown();
                    updater.removeFile("a/b/c/d/2.txt");
                }));
                futures.add(executor.submit(() -> {
                    latch.countDown();
                    updater.writeFile(ITestHelper.streamString("test".repeat(100)), "test.txt");
                }));
                futures.add(executor.submit(() -> {
                    latch.countDown();
                    updater.renameFile("a/b/c/d/4.txt", "a/b/c/d/1.txt", OcflOption.OVERWRITE);
                }));
                futures.add(executor.submit(() -> {
                    latch.countDown();
                    updater.writeFile(ITestHelper.streamString("new".repeat(100)), "a/new.txt");
                }));

                joinFutures(futures);

                assertEquals(4, errors.get(), "4 out of 5 renames should have failed");
            });

            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("3"), updater -> {
                var latch = new CountDownLatch(5);
                var futures = new ArrayList<Future<?>>();

                for (int i = 0; i < 5; i++) {
                    futures.add(executor.submit(() -> {
                        latch.countDown();
                        updater.addPath(ITestHelper.expectedRepoPath("repo15"), "repo15", OcflOption.OVERWRITE);
                    }));
                }

                joinFutures(futures);
            });

            repo.updateObject(ObjectVersionId.head(objectId), defaultVersionInfo.setMessage("4"), updater -> {
                var root = ITestHelper.expectedRepoPath("repo17");
                var futures = new ArrayList<Future<?>>();

                try (var files = Files.find(root, Integer.MAX_VALUE, (file, attrs) -> attrs.isRegularFile())) {
                    files.map(file -> executor.submit(() -> updater.addPath(
                                    file, "repo17/" + FileUtil.pathToStringStandardSeparator(root.relativize(file)))))
                            .forEach(futures::add);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

                joinFutures(futures);
            });

            Assertions.assertThat(repo.validateObject(objectId, true).getErrors())
                    .isEmpty();

            var outputPath1 = outputPath(repoName, objectId + "v1");
            repo.getObject(ObjectVersionId.version(objectId, 1), outputPath1);
            ITestHelper.verifyDirectoryContentsSame(ITestHelper.expectedOutputPath(repoName, "o1v1"), outputPath1);

            var outputPath2 = outputPath(repoName, objectId + "v2");
            repo.getObject(ObjectVersionId.version(objectId, 2), outputPath2);
            ITestHelper.verifyDirectoryContentsSame(ITestHelper.expectedOutputPath(repoName, "o1v2"), outputPath2);

            var outputPath3 = outputPath(repoName, objectId + "v3");
            repo.getObject(ObjectVersionId.version(objectId, 3), outputPath3);
            ITestHelper.verifyDirectoryContentsSame(ITestHelper.expectedOutputPath(repoName, "o1v3"), outputPath3);

            var outputPath4 = outputPath(repoName, objectId + "v4");
            repo.getObject(ObjectVersionId.version(objectId, 4), outputPath4);
            ITestHelper.verifyDirectoryContentsSame(ITestHelper.expectedOutputPath(repoName, "o1v4"), outputPath4);
        } finally {
            executor.shutdownNow();
        }
    }

    private void joinFutures(List<Future<?>> futures) {
        for (var future : futures) {
            try {
                future.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    protected OcflRepository defaultRepo(String name, Consumer<OcflRepositoryBuilder> consumer) {
        var builder = new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .inventoryCache(new NoOpCache<>())
                .objectLock(lock -> lock.dataSource(dataSource).tableName(lockTable()))
                .objectDetailsDb(db -> db.dataSource(dataSource).tableName(detailsTable()))
                .inventoryMapper(ITestHelper.testInventoryMapper())
                .contentPathConstraints(ContentPathConstraints.cloud())
                .storage(storage ->
                        storage.objectMapper(ITestHelper.prettyPrintMapper()).cloud(createCloudClient(name)))
                .workDir(workDir);

        consumer.accept(builder);

        var repo = builder.build();
        ITestHelper.fixTime(repo, "2019-08-05T15:57:53Z");
        return repo;
    }

    @Override
    protected OcflRepository existingRepo(String name, Path path, Consumer<OcflRepositoryBuilder> consumer) {
        var client = createCloudClient(name);
        FileUtil.findFiles(path).stream()
                .filter(f -> !f.getFileName().toString().equals(".gitkeep"))
                .forEach(file -> {
                    client.uploadFile(file, FileUtil.pathToStringStandardSeparator(path.relativize(file)));
                });
        return defaultRepo(name, consumer);
    }

    @Override
    protected void verifyRepo(String name) {
        s3Helper.verifyRepo(ITestHelper.expectedRepoPath(name), bucket, prefix(name));
    }

    @Override
    protected List<String> listFilesInRepo(String name) {
        return s3Helper.listAllObjects(bucket, prefix(name));
    }

    private String detailsTable() {
        return "details_" + UUID.randomUUID().toString().replaceAll("-", "");
    }

    private String lockTable() {
        return "lock_" + UUID.randomUUID().toString().replaceAll("-", "");
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
