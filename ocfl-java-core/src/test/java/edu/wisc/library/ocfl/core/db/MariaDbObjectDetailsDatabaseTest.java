package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.mariadb.jdbc.MariaDbPoolDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
@Disabled // Run this test manually as it requires Docker and Testcontainers setup + takes long time.
class MariaDbObjectDetailsDatabaseTest {

    @TempDir
    public Path tempDir;

    @Container
    private static final MariaDBContainer<?> mariaDB = new MariaDBContainer<>(DockerImageName.parse("mariadb:latest"));
    private static MariaDbPoolDataSource dataSource;

    private MariaDbObjectDetailsDatabase cut;

    private String tableName;
    private InventoryMapper inventoryMapper;
    private ExecutorService executor;

    @BeforeAll
    public static void beforeAll() throws SQLException {
        System.out.printf("%n%n%n%n=========================================%n======= Starting MariaDB UnitTest =======%n=========================================%n%n%n%n");

        dataSource = new MariaDbPoolDataSource(mariaDB.getJdbcUrl());
        dataSource.setUser(mariaDB.getUsername());
        dataSource.setPassword(mariaDB.getPassword());
    }

    @BeforeEach
    public void setup() {
        tableName = "details_" + UUID.randomUUID().toString().replaceAll("-", "");

        cut = createDatabase(tableName);
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

        cut.addObjectDetails(inventory, digest, invBytes);
        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldUpdateDetailsWhenDetailsExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

        inventory = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        invBytes = inventoryBytes(inventory);
        digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);
        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldReturnNullWhenDetailsDoNotExist() {
        var details = cut.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @Test
    public void shouldApplyUpdateWhenRunnableSucceeds() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

        inventory = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        invBytes = inventoryBytes(inventory);
        var invPath = writeInventory(invBytes);
        digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.updateObjectDetails(inventory, digest, invPath, () -> {
        });
        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldRollbackDbChangesWhenRunnableFails() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

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
            cut.updateObjectDetails(inv2, digest2, invPath, () -> {
                throw new RuntimeException("Failure!");
            });
        });

        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldDeleteDetailsWhenExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);
        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);

        cut.deleteObjectDetails(inventory.getId());
        details = cut.retrieveObjectDetails(inventory.getId());

        assertNull(details);
    }

    @Test
    public void shouldDeleteAllDetailsWhenExist() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);
        var details = cut.retrieveObjectDetails(inventory.getId());

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

        cut.addObjectDetails(inv2, digest2, invBytes2);
        details = cut.retrieveObjectDetails(inv2.getId());

        assertObjectDetails(inv2, digest2, invBytes2, details);

        cut.deleteAllDetails();

        assertNull(cut.retrieveObjectDetails(inventory.getId()));
        assertNull(cut.retrieveObjectDetails(inv2.getId()));
    }

    @Test
    public void shouldDoNothingWhenDeleteAndDetailsDoNotExist() {
        cut.deleteObjectDetails("o1");
        var details = cut.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @Test
    public void shouldNotStoreInventoryBytesWhenFeatureDisabled() {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
                .storeInventory(false)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);
        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, null, details);
    }

    @Test
    public void shouldRejectUpdateWhenNewInventoryVersionIsNotNextVersion() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

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
            cut.addObjectDetails(inv2, digest2, invBytes2);
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

        cut.addObjectDetails(inv2, digest2, invBytes2);

        assertThrows(ObjectOutOfSyncException.class, () -> {
            cut.addObjectDetails(inventory, digest, invBytes);
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

        cut.addObjectDetails(inventory, digest, invBytes);

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
            cut.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldRejectUpdateWhenNewRevisionButNotR1() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

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
            cut.addObjectDetails(inv2, digest2, invBytes2);
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

        cut.addObjectDetails(inventory, digest, invBytes);

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
            cut.addObjectDetails(inv2, digest2, invBytes2);
        });
    }

    @Test
    public void shouldFailWhenCannotAcquireLock() throws InterruptedException, ExecutionException {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
                .waitTime(1, TimeUnit.SECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);
        var invPath = writeInventory(invBytes);

        var update1 = new Thread(() ->
                assertThrows(LockException.class, () ->
                        cut.updateObjectDetails(inventory, digest, invPath, () -> {
                            try {
                                TimeUnit.SECONDS.sleep(2);
                            } catch (InterruptedException e) {
                                Thread.interrupted();
                            }
                        })));

        var update2 = new Thread(() -> cut.addObjectDetails(inventory, digest, invBytes));

        update1.start();
        update2.start();
    }

    @Test
    public void shouldFailDeleteWhenCannotAcquireLock() throws InterruptedException, ExecutionException {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
                .waitTime(10, TimeUnit.SECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        cut.addObjectDetails(inventory, digest, invBytes);

        var inv2 = inventory.buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);
        var invPath = writeInventory(invBytes2);

        var update1 = new Thread(() -> cut.updateObjectDetails(inv2, digest2, invPath, () -> {
            try {
                TimeUnit.SECONDS.sleep(5);
            } catch (InterruptedException e) {
            }
        }));

        var update2 = new Thread(() -> assertThrows(LockException.class, () -> cut.deleteObjectDetails(inventory.getId())));

        update1.start();
        update2.start();
    }

    @Test
    public void shouldFailWhenConcurrentUpdateAndNew() throws InterruptedException, ExecutionException {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
                .waitTime(1, TimeUnit.SECONDS)
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

        var update1 = new Thread(() -> cut.updateObjectDetails(inv2, digest2, invPath2, () -> {
            try {
                TimeUnit.SECONDS.sleep(2);
            } catch (InterruptedException e) {
            } finally {
                phaser.arriveAndAwaitAdvance();
            }
        }));

        var update2 = new Thread(() -> assertThrows(LockException.class, () -> cut.updateObjectDetails(inventory, digest, invPath, () -> {
        })));

        update1.start();
        update2.start();

        phaser.arriveAndAwaitAdvance();

        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inv2, digest2, invBytes2, details);
    }

    @Test
    public void shouldSucceedWhenConcurrentAddAndSameDigest() throws InterruptedException, ExecutionException {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
                .waitTime(5, TimeUnit.SECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var phaser = new Phaser(2);

        var update1 = new Thread(() -> cut.addObjectDetails(inventory, digest, invBytes));
        var update2 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                cut.addObjectDetails(inventory, digest, invBytes);
            } catch (InterruptedException e) {
            } finally {
                phaser.arriveAndAwaitAdvance();
            }
        });

        update1.start();
        update2.start();

        phaser.arriveAndAwaitAdvance();

        var details = cut.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldFailWhenConcurrentAddAndDifferentDigest() {
        cut = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder()
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

        var update1 = new Thread(() -> cut.addObjectDetails(inventory, digest, invBytes));
        var update2 = new Thread(() -> {
            try {
                TimeUnit.SECONDS.sleep(1);
                assertThrows(ObjectOutOfSyncException.class, () -> cut.addObjectDetails(inv2, digest2, invBytes2));
            } catch (InterruptedException e) {
            }
        });

        update1.start();
        update2.start();
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

    private MariaDbObjectDetailsDatabase createDatabase(String tableName) {
        return (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder().dataSource(dataSource).tableName(tableName).build();
    }
}
