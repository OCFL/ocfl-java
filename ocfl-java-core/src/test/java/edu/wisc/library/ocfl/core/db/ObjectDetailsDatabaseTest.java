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
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.OffsetDateTime;
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

    public static final String TABLE_1 = "ocfl_object_details";
    public static final String TABLE_2 = "obj_details_2";
    
    @TempDir
    public Path tempDir;

    private String tableName;
    private ComboPooledDataSource dataSource;
    private InventoryMapper inventoryMapper;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        dataSource = new ComboPooledDataSource();
        dataSource.hardReset();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");

        inventoryMapper = InventoryMapper.prettyPrintMapper();
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void after() {
        truncateObjectDetails();
        dataSource.hardReset();
        executor.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldAddDetailsWhenDoNotExist(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldUpdateDetailsWhenDetailsExist(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldReturnNullWhenDetailsDoNotExist(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
        var details = database.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldApplyUpdateWhenRunnableSucceeds(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRollbackDbChangesWhenRunnableFails(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldDeleteDetailsWhenExist(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldDoNothingWhenDeleteAndDetailsDoNotExist(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
        database.deleteObjectDetails("o1");
        var details = database.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldNotStoreInventoryBytesWhenFeatureDisabled(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
        database = new ObjectDetailsDatabaseBuilder().storeInventory(false).dataSource(dataSource).build();

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);
        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, null, details);
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRejectUpdateWhenNewInventoryVersionIsNotNextVersion(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRejectUpdateWhenNewInventoryVersionIsOldVersion(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRejectUpdateWhenNewRevisionButNotR1(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRejectUpdateWhenRevisionAndUpdateDifferentVersion(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldRejectUpdateWhenRevisionAndUpdateNotNextRevision(String tableName) {
        this.tableName = tableName;
        var database = createDatabase(tableName);
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldFailWhenCannotAcquireLock(String tableName) throws InterruptedException, ExecutionException {
        this.tableName = tableName;
        var database = new ObjectDetailsDatabaseBuilder()
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldFailDeleteWhenCannotAcquireLock(String tableName) throws InterruptedException, ExecutionException {
        this.tableName = tableName;
        var database = new ObjectDetailsDatabaseBuilder()
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldFailWhenConcurrentUpdateAndNew(String tableName) throws InterruptedException, ExecutionException {
        this.tableName = tableName;
        var database = new ObjectDetailsDatabaseBuilder()
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldSucceedWhenConcurrentAddAndSameDigest(String tableName) throws InterruptedException, ExecutionException {
        this.tableName = tableName;
        var database = new ObjectDetailsDatabaseBuilder()
                .waitTime(500, TimeUnit.MILLISECONDS)
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

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldFailWhenConcurrentAddAndDifferentDigest(String tableName) {
        this.tableName = tableName;
        var database = new ObjectDetailsDatabaseBuilder()
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

    private void truncateObjectDetails() {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("TRUNCATE TABLE " + tableName)) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ObjectDetailsDatabase createDatabase(String tableName) {
        // want to make sure the defaulting works
        var name = TABLE_1.equals(tableName) ? null : tableName;
        return new ObjectDetailsDatabaseBuilder().dataSource(dataSource).tableName(name).build();
    }

}
