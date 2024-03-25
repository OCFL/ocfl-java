package io.ocfl.itest;

import io.micrometer.core.instrument.Metrics;
import io.ocfl.api.MutableOcflRepository;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.aws.OcflS3Client;
import io.ocfl.core.OcflRepositoryBuilder;
import io.ocfl.core.cache.NoOpCache;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import io.ocfl.core.util.FileUtil;
import io.ocfl.core.util.UncheckedFiles;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import org.HdrHistogram.Histogram;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3AsyncClient;
import software.amazon.awssdk.transfer.s3.S3TransferManager;

@Disabled
public class LoadITest {

    private static final int KB = 1024;
    private static final long MB = 1024 * KB;

    private static final int BUFFER_SIZE = 32 * KB;

    @TempDir
    public Path tempRoot;

    private static final Histogram histogram = new Histogram(3600000000000L, 3);

    @Test
    public void fsPutObjectSmallFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(7);
        var fileCount = 10;
        var fileSize = 128 * KB;

        var repo = createFsRepo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "fs", true);
    }

    @Test
    public void fsPutObjectModestFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(7);
        var fileCount = 10;
        var fileSize = 3 * MB;

        var repo = createFsRepo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "fs", true);
    }

    @Test
    public void fsPutObjectManyFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(10);
        var fileCount = 2048;
        var fileSize = 128 * KB;

        var repo = createFsRepo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "fs", true);
    }

    @Test
    public void fsPutObjectLargeFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 2;
        var fileSize = 128 * MB;

        var repo = createFsRepo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "fs", true);
    }

    @Test
    public void s3PutObjectModestFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 10;
        var fileSize = 3 * MB;

        var repo = createS3Repo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "s3", false);
        // TODO Don't forget to delete from S3!
    }

    @Test
    public void s3PutObjectManyFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 2048;
        var fileSize = 128 * KB;

        var repo = createS3Repo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "s3", false);
        // TODO Don't forget to delete from S3!
    }

    @Test
    public void s3PutObjectLargeFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 2;
        var fileSize = 128 * MB;

        var repo = createS3Repo();

        runPutTest(repo, fileCount, fileSize, threadCount, duration, "s3", false);
        // TODO Don't forget to delete from S3!
    }

    @Test
    public void fsGetObjectModestFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 10;
        var fileSize = 3 * MB;

        var repo = createFsRepo();

        runGetTest(repo, fileCount, fileSize, threadCount, duration, "fs");
    }

    @Test
    public void s3GetObjectModestFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 10;
        var fileSize = 3 * MB;

        var repo = createS3Repo();

        runGetTest(repo, fileCount, fileSize, threadCount, duration, "s3");
        // TODO Don't forget to delete from S3!
    }

    @Test
    public void s3MutableHeadCopyObjectModestFilesTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var fileCount = 10;
        var fileSize = 3 * MB;

        var repo = createS3Repo();

        System.out.println("Starting putTest");

        System.out.println("Creating test object");
        var objectPath = createTestObject(fileCount, fileSize);
        System.out.println("Created test object: " + objectPath);

        var versionInfo =
                new VersionInfo().setUser("Peter", "pwinckles@example.com").setMessage("Testing");

        var timer = Metrics.timer(
                "putObject",
                "files",
                String.valueOf(fileCount),
                "sizeBytes",
                String.valueOf(fileSize),
                "threads",
                String.valueOf(threadCount),
                "storage",
                "s3");

        var threads = new ArrayList<Thread>(threadCount);

        for (var i = 0; i < threadCount; i++) {
            threads.add(createThread(duration, objectId -> {
                repo.stageChanges(ObjectVersionId.head(objectId), versionInfo, updater -> {
                    updater.addPath(objectPath);
                });
                timer.record(() -> {
                    repo.commitStagedChanges(objectId, versionInfo);
                });
            }));
        }

        startThreads(threads);
        System.out.println("Waiting for threads to complete...");
        joinThreads(threads);

        System.out.println("Finished. Waiting for metrics collection...");
        TimeUnit.SECONDS.sleep(30);
        System.out.println("Done");
        // TODO Don't forget to delete from S3!
    }

    @Test
    public void s3WriteTest() throws InterruptedException {
        var threadCount = 10;
        var duration = Duration.ofMinutes(15);
        var objectPath = createTestObject(1, 3 * MB);
        var prefix = UUID.randomUUID().toString();

        var s3Client = S3AsyncClient.crtBuilder().region(Region.US_EAST_2).build();
        var transferManager = S3TransferManager.builder().s3Client(s3Client).build();
        var cloutClient = OcflS3Client.builder()
                .s3Client(s3Client)
                .transferManager(transferManager)
                .bucket("pwinckles-ocfl")
                .repoPrefix(prefix)
                .build();

        var timer = Metrics.timer("putObjectDirect", "threads", String.valueOf(threadCount), "storage", "s3");

        var threads = new ArrayList<Thread>(threadCount);

        for (var i = 0; i < threadCount; i++) {
            threads.add(createThread(duration, objectId -> {
                timer.record(() -> {
                    cloutClient.uploadFile(objectPath.resolve("file-0"), objectId + "/file-0");
                });
            }));
        }

        startThreads(threads);
        System.out.println("Waiting for threads to complete...");
        joinThreads(threads);

        System.out.println("Finished. Waiting for metrics collection...");
        TimeUnit.SECONDS.sleep(30);
        System.out.println("Done");

        s3Client.close();
        transferManager.close();
    }

    private void runPutTest(
            OcflRepository repo,
            int fileCount,
            long fileSize,
            int threadCount,
            Duration duration,
            String storageType,
            boolean shouldPurge)
            throws InterruptedException {
        System.out.println("Starting putTest");
        histogram.reset();

        System.out.println("Creating test object");
        var objectPath = createTestObject(fileCount, fileSize);
        System.out.println("Created test object: " + objectPath);

        var versionInfo =
                new VersionInfo().setUser("Peter", "pwinckles@example.com").setMessage("Testing");

        var threads = new ArrayList<Thread>(threadCount);

        for (var i = 0; i < threadCount; i++) {
            threads.add(createThread(duration, objectId -> {
                var start = System.nanoTime();
                repo.putObject(ObjectVersionId.head(objectId), objectPath, versionInfo);
                var end = System.nanoTime();
                histogram.recordValue(end - start);
                if (shouldPurge) {
                    repo.purgeObject(objectId);
                }
            }));
        }

        startThreads(threads);
        System.out.println("Waiting for threads to complete...");
        joinThreads(threads);
        System.out.println("Done");

        System.out.printf(
                "putTest results for %s files=%d size=%s threads=%s%n", storageType, fileSize, fileCount, threadCount);
        histogram.outputPercentileDistribution(System.out, 1_000_000.0);
    }

    private void runGetTest(
            OcflRepository repo, int fileCount, long fileSize, int threadCount, Duration duration, String storageType)
            throws InterruptedException {
        System.out.println("Starting getTest");
        histogram.reset();

        System.out.println("Creating test object");
        var objectPath = createTestObject(fileCount, fileSize);
        System.out.println("Created test object: " + objectPath);

        var versionInfo =
                new VersionInfo().setUser("Peter", "pwinckles@example.com").setMessage("Testing");

        var objectId = UUID.randomUUID().toString();

        repo.putObject(ObjectVersionId.head(objectId), objectPath, versionInfo);

        var threads = new ArrayList<Thread>(threadCount);

        for (var i = 0; i < threadCount; i++) {
            threads.add(createThread(duration, out -> {
                var outDir = tempRoot.resolve(out);
                var start = System.nanoTime();
                repo.getObject(ObjectVersionId.head(objectId), outDir);
                var end = System.nanoTime();
                histogram.recordValue(end - start);
                FileUtil.safeDeleteDirectory(outDir);
            }));
        }

        startThreads(threads);
        System.out.println("Waiting for threads to complete...");
        joinThreads(threads);
        System.out.println("Done");

        System.out.printf(
                "getTest results for %s files=%d size=%s threads=%s%n", storageType, fileSize, fileCount, threadCount);
        histogram.outputPercentileDistribution(System.out, 1_000_000.0);
    }

    private Thread createThread(Duration duration, Consumer<String> test) {
        return new Thread() {
            private final String id = UUID.randomUUID().toString();

            @Override
            public void run() {
                System.out.println("Starting thread " + id);
                var count = 0;
                var start = Instant.now();

                while (Duration.between(start, Instant.now()).compareTo(duration) < 0) {
                    if (Thread.interrupted()) {
                        break;
                    }
                    var objectId = id + "-" + count++;
                    try {
                        test.accept(objectId);
                    } catch (RuntimeException e) {
                        System.err.println("Exception in thread: " + id);
                        e.printStackTrace(System.err);
                    }
                }

                System.out.println("Completed thread " + id);
            }
        };
    }

    private void startThreads(List<Thread> threads) {
        for (var thread : threads) {
            thread.start();
        }
    }

    private void joinThreads(List<Thread> threads) throws InterruptedException {
        for (var thread : threads) {
            thread.join();
        }
    }

    private OcflRepository createFsRepo() {
        System.out.println("OCFL root: " + tempRoot.toString() + "/ocfl");
        return new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .inventoryCache(new NoOpCache<>())
                .storage(storage -> storage.fileSystem(UncheckedFiles.createDirectories(tempRoot.resolve("ocfl"))))
                .workDir(UncheckedFiles.createDirectories(tempRoot.resolve("temp")))
                .build();
    }

    private MutableOcflRepository createS3Repo() {
        //        var s3Client = S3AsyncClient.builder()
        //                .region(Region.US_EAST_2)
        //                .httpClientBuilder(NettyNioAsyncHttpClient.builder()
        //                        .connectionAcquisitionTimeout(Duration.ofSeconds(60))
        //                        .writeTimeout(Duration.ofSeconds(0))
        //                        .readTimeout(Duration.ofSeconds(0))
        //                        .maxConcurrency(100))
        //                .build();
        //        var transferManager = S3TransferManager.builder()
        //                .s3Client(MultipartS3AsyncClient.create(
        //                        s3Client, MultipartConfiguration.builder().build()))
        //                .build();

        var s3Client = S3AsyncClient.crtBuilder().region(Region.US_EAST_2).build();
        var transferManager = S3TransferManager.builder().s3Client(s3Client).build();

        var prefix = UUID.randomUUID().toString();
        // Note this is NOT using a db, which an S3 setup would normally use
        return new OcflRepositoryBuilder()
                .defaultLayoutConfig(new HashedNTupleLayoutConfig())
                .inventoryCache(new NoOpCache<>())
                .storage(storage -> {
                    storage.cloud(OcflS3Client.builder()
                            .bucket("pwinckles-ocfl")
                            .repoPrefix(prefix)
                            .s3Client(s3Client)
                            .transferManager(transferManager)
                            .build());
                })
                .workDir(UncheckedFiles.createDirectories(tempRoot.resolve("temp")))
                .buildMutable();
    }

    private Path createTestObject(int fileCount, long fileSize) {
        var uuid = UUID.randomUUID().toString();
        var objectPath = UncheckedFiles.createDirectories(tempRoot.resolve(uuid));

        for (int i = 0; i < fileCount; i++) {
            writeFile(objectPath.resolve("file-" + i), fileSize);
        }

        return objectPath;
    }

    private void writeFile(Path path, long size) {
        var bytes = new byte[BUFFER_SIZE];
        try (var out = new BufferedOutputStream(Files.newOutputStream(path))) {
            var written = 0;
            while (written < size) {
                ThreadLocalRandom.current().nextBytes(bytes);
                out.write(bytes);
                written += BUFFER_SIZE;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
