package edu.wisc.library.ocfl.core.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ObjectDetailsDatabaseTest {

    @TempDir
    public Path tempDir;

    private static ComboPooledDataSource dataSource;

    private String tableName;
    private InventoryMapper inventoryMapper;
    private ExecutorService executor;
    private ObjectDetailsDatabase database;

    @BeforeAll
    public static void beforeAll() {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(System.getProperty("db.url", "jdbc:h2:mem:test"));
        dataSource.setUser(System.getProperty("db.user", ""));
        dataSource.setPassword(System.getProperty("db.password", ""));
    }

    @BeforeEach
    public void setup() {
        tableName = "details_" + UUID.randomUUID().toString().replaceAll("-", "");

        database = createDatabase(tableName);
        inventoryMapper = InventoryMapper.prettyPrintMapper();
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void after() {
        executor.shutdown();
    }

    @Test
    public void shouldAddDetailsWhenDoNotExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldUpdateDetailsWhenDetailsExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        inventory = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        invBytes = inventoryBytes(inventory);
        digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldReturnNullWhenDetailsDoNotExist() {
        var details = database.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @Test
    public void shouldApplyUpdateWhenRunnableSucceeds() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        inventory = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        invBytes = inventoryBytes(inventory);
        var invPath = writeInventory(invBytes);
        digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.updateObjectDetails(inventory, digest, invPath, () -> {});
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldRollbackDbChangesWhenRunnableFails() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inventory);
        var invPath = writeInventory(invBytes2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        OcflAsserts.assertThrowsWithMessage(RuntimeException.class, "Failure!", () -> {
            database.updateObjectDetails(inv2, digest2, invPath, () -> {
                throw new RuntimeException("Failure!");
            });
        });

        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldDeleteDetailsWhenExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);

        database.deleteObjectDetails(inventory.getId());
        details = database.retrieveObjectDetails(inventory.getId());

        assertNull(details);
    }

    @Test
    public void shouldDeleteAllDetailsWhenExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);

        var inv2 = Inventory.builderFromStub("o2", new OcflConfig(), "o2")
                .addFileToManifest("f1", "v1/content/file1.txt")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file1.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        database.addObjectDetails(inv2, digest2, invBytes2);
        details = database.retrieveObjectDetails(inv2.getId());

        assertObjectDetails(inv2, digest2, invBytes2, details);

        database.deleteAllDetails();

        assertNull(database.retrieveObjectDetails(inventory.getId()));
        assertNull(database.retrieveObjectDetails(inv2.getId()));
    }

    @Test
    public void shouldDoNothingWhenDeleteAndDetailsDoNotExist() {
        database.deleteObjectDetails("o1");
        var details = database.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @Test
    public void shouldNotStoreInventoryBytesWhenFeatureDisabled() {
        database = new ObjectDetailsDatabaseBuilder()
                .storeInventory(false)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, null, details);
    }

    @Test
    public void shouldRejectUpdateWhenNewInventoryVersionIsNotNextVersion() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file3.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldRejectUpdateWhenNewInventoryVersionIsOldVersion() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file3.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        database.addObjectDetails(inv2, digest2, invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            database.addObjectDetails(inventory, digest, invBytes);
        });
    }

    @Test
    public void shouldRejectUpdateWhenRevisionAndUpdateDifferentVersion() {
        var inventory = Inventory.builderFromStub("o1", new OcflConfig(), "o1")
                .mutableHead(true)
                .addFileToManifest("f1", "v1/content/file1.txt")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file1.txt")
                        .build())
                .build();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .mutableHead(false)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldRejectUpdateWhenNewRevisionButNotR1() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .mutableHead(true)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file3.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldRejectUpdateWhenRevisionAndUpdateNotNextRevision() {
        var inventory = Inventory.builderFromStub("o1", new OcflConfig(), "o1")
                .mutableHead(true)
                .addFileToManifest("f1", "v1/content/file1.txt")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file1.txt")
                        .build())
                .build();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file3.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldFailWhenCannotAcquireLock() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder()
                .waitTime(250, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);
        var invPath = writeInventory(invBytes);

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inventory, digest, invPath, () -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        assertThrows(LockException.class, () -> {
            database.addObjectDetails(inventory, digest, invBytes);
        });

        future.get();
    }

    @Test
    public void shouldFailDeleteWhenCannotAcquireLock() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder()
                .waitTime(250, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);
        var invPath = writeInventory(invBytes2);

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inv2, digest2, invPath, () -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    TimeUnit.MILLISECONDS.sleep(2000);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        assertThrows(LockException.class, () -> {
            database.deleteObjectDetails(inventory.getId());
        });

        future.get();
    }

    @Test
    public void shouldFailWhenConcurrentUpdateAndNew() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder()
                .waitTime(250, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);
        var invPath = writeInventory(invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);
        var invPath2 = writeInventory(invBytes2);

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inv2, digest2, invPath2, () -> {
                try {
                    phaser.arriveAndAwaitAdvance();
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        assertThrows(LockException.class, () -> {
            database.updateObjectDetails(inventory, digest, invPath, () -> {});
        });

        future.get();

        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inv2, digest2, invBytes2, details);
    }

    @Test
    public void shouldSucceedWhenConcurrentAddAndSameDigest() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder()
                .waitTime(1000, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            database.addObjectDetails(inventory, digest, invBytes);
        });

        phaser.arriveAndAwaitAdvance();
        database.addObjectDetails(inventory, digest, invBytes);

        future.get();

        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldFailWhenConcurrentAddAndDifferentDigest() {
        database = new ObjectDetailsDatabaseBuilder()
                .waitTime(1, TimeUnit.SECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var inv2 = inventory.buildFrom().build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = "bogus";

        var phaser = new Phaser(3);

        var future = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
        var future2 = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            database.addObjectDetails(inventory, digest, invBytes);
        });

        phaser.arriveAndAwaitAdvance();

        assertThrows(ObjectOutOfSyncException.class, () -> {
            try {
                future.get();
                future2.get();
            } catch (ExecutionException e) {
                throw e.getCause();
            }
        });
    }

    private void assertObjectDetails(Inventory inventory, String inventoryDigest, byte[] inventoryBytes, OcflObjectDetails details) {
        assertEquals(inventory.getId(), details.getObjectId());
        assertEquals(inventory.getHead(), details.getVersionNum());
        assertEquals(inventoryDigest, details.getInventoryDigest());
        assertEquals(inventory.getObjectRootPath(), details.getObjectRootPath());
        assertEquals(inventory.getDigestAlgorithm(), details.getDigestAlgorithm());
        assertEquals(inventory.getRevisionNum(), details.getRevisionNum());
        assertNotNull(details.getUpdateTimestamp());
        assertArrayEquals(inventoryBytes, details.getInventoryBytes());
    }

    private Inventory basicInventory() {
        return Inventory.builderFromStub("o1", new OcflConfig(), "o1")
                .addFileToManifest("f1", "v1/content/file1.txt")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file1.txt")
                        .build())
                .build();
    }

    private byte[] inventoryBytes(Inventory inventory) {
        var baos = new ByteArrayOutputStream();
        inventoryMapper.write(baos, inventory);
        return baos.toByteArray();
    }

    private Path writeInventory(byte[] invBytes) {
        try {
            var dst = tempDir.resolve("inventory.json");
            Files.write(dst, invBytes);
            return dst;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private ObjectDetailsDatabase createDatabase(String tableName) {
        return new ObjectDetailsDatabaseBuilder().dataSource(dataSource).tableName(tableName).build();
    }

}
