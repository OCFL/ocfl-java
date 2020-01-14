package edu.wisc.library.ocfl.core.db;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.core.OcflConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import javax.sql.DataSource;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

public class ObjectDetailsDatabaseTest {

    @TempDir
    public Path tempDir;

    private ObjectDetailsDatabase database;
    private ComboPooledDataSource dataSource;
    private InventoryMapper inventoryMapper;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");
//        dataSource.setJdbcUrl( "jdbc:postgresql://localhost/ocfl" );
//        dataSource.setUser("pwinckles");
//        dataSource.setPassword("");

        database = new ObjectDetailsDatabaseBuilder().build(dataSource);
        inventoryMapper = InventoryMapper.prettyPrintMapper();
        executor = Executors.newFixedThreadPool(2);

        truncateObjectDetails(dataSource);
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

        inventory = Inventory.builder(inventory)
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

        inventory = Inventory.builder(inventory)
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

        var inv2 = Inventory.builder(inventory)
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
    public void shouldDoNothingWhenDeleteAndDetailsDoNotExist() {
        database.deleteObjectDetails("o1");
        var details = database.retrieveObjectDetails("o1");
        assertNull(details);
    }

    @Test
    public void shouldNotStoreInventoryBytesWhenFeatureDisabled() {
        database = new ObjectDetailsDatabaseBuilder().storeInventory(false).build(dataSource);

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

        var inv2 = Inventory.builder(inventory)
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

        var inv2 = Inventory.builder(inventory)
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
    public void shouldRejectUpdateWhenNewRevisionButNotR1() {
        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = Inventory.builder(inventory)
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

        var inv2 = Inventory.builder(inventory)
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

        var inv2 = Inventory.builder(inventory)
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
        database = new ObjectDetailsDatabaseBuilder().waitTime(500, TimeUnit.MILLISECONDS).build(dataSource);

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);
        var invPath = writeInventory(invBytes);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inventory, digest, invPath, () -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        assertThrows(LockException.class, () -> {
            database.addObjectDetails(inventory, digest, invBytes);
        });

        future.get();
    }

    @Test
    public void shouldFailDeleteWhenCannotAcquireLock() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder().waitTime(500, TimeUnit.MILLISECONDS).build(dataSource);

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        database.addObjectDetails(inventory, digest, invBytes);

        var inv2 = Inventory.builder(inventory)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);
        var invPath = writeInventory(invBytes2);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inv2, digest2, invPath, () -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        assertThrows(LockException.class, () -> {
            database.deleteObjectDetails(inventory.getId());
        });

        future.get();
    }

    @Test
    public void shouldFailWhenConcurrentUpdateAndNew() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder().waitTime(500, TimeUnit.MILLISECONDS).build(dataSource);

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);
        var invPath = writeInventory(invBytes);

        var inv2 = Inventory.builder(inventory)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);
        var invPath2 = writeInventory(invBytes2);

        var future = executor.submit(() -> {
            database.updateObjectDetails(inv2, digest2, invPath2, () -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        assertThrows(LockException.class, () -> {
            database.updateObjectDetails(inventory, digest, invPath, () -> {
                try {
                    Thread.sleep(TimeUnit.SECONDS.toMillis(3));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        future.get();

        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inv2, digest2, invBytes2, details);
    }

    @Test
    public void shouldSucceedWhenConcurrentAddAndSameDigest() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder().waitTime(500, TimeUnit.MILLISECONDS).build(dataSource);

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var future = executor.submit(() -> {
            database.addObjectDetails(inventory, digest, invBytes);
        });

        database.addObjectDetails(inventory, digest, invBytes);

        future.get();

        var details = database.retrieveObjectDetails(inventory.getId());

        assertObjectDetails(inventory, digest, invBytes, details);
    }

    @Test
    public void shouldFailWhenConcurrentAddAndDifferentDigest() throws InterruptedException, ExecutionException {
        database = new ObjectDetailsDatabaseBuilder().waitTime(500, TimeUnit.MILLISECONDS).build(dataSource);

        var inventory = basicInventory();
        var invBytes = inventoryBytes(inventory);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        var inv2 = Inventory.builder(inventory)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .build())
                .build();
        var invBytes2 = inventoryBytes(inv2);
        var digest2 = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes2);

        var future = executor.submit(() -> {
            database.addObjectDetails(inv2, digest2, invBytes2);
        });
        var future2 = executor.submit(() -> {
            database.addObjectDetails(inventory, digest, invBytes);
        });

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
        assertEquals(inventory.getHead(), details.getVersionId());
        assertEquals(inventoryDigest, details.getInventoryDigest());
        assertEquals(inventory.getObjectRootPath(), details.getObjectRootPath());
        assertEquals(inventory.getDigestAlgorithm(), details.getDigestAlgorithm());
        assertEquals(inventory.getRevisionId(), details.getRevisionId());
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

    private void truncateObjectDetails(DataSource dataSource) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("TRUNCATE TABLE ocfl_object_details")) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
