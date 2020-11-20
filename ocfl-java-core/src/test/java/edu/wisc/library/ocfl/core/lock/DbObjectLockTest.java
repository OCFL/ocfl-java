package edu.wisc.library.ocfl.core.lock;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.exception.LockException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.sql.SQLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class DbObjectLockTest {

    public static final String TABLE_1 = "ocfl_object_lock";
    public static final String TABLE_2 = "obj_lock_2";

    private String tableName;
    private ComboPooledDataSource dataSource;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");

        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void after() {
        truncateObjectLock();
        dataSource.hardReset();
        executor.shutdown();
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldAcquireLockWhenDoesNotExist(String tableName) {
        this.tableName = tableName;
        var lock = createLock(tableName);
        var result = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result.set(true);
        });
        assertTrue(result.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldAcquireLockWhenAlreadyExistsButNotHeld(String tableName) {
        this.tableName = tableName;
        var lock = createLock(tableName);
        var result = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result.set(true);
        });
        assertTrue(result.get());

        lock.doInWriteLock("obj1", () -> {
            result.set(false);
        });
        assertFalse(result.get());
    }

    @ParameterizedTest
    @ValueSource(strings = {TABLE_1, TABLE_2})
    public void shouldThrowExceptionWhenCannotAcquireLock(String tableName) throws ExecutionException, InterruptedException {
        this.tableName = tableName;
        var lock = createLock(tableName);

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            lock.doInWriteLock("obj1", () -> {
                phaser.arriveAndAwaitAdvance();
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(500));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        var result = new AtomicBoolean(false);
        assertThrows(LockException.class, () -> {
            lock.doInWriteLock("obj1", () -> {
                result.set(true);
            });
        });

        assertFalse(result.get());
        future.get();
    }

    private void truncateObjectLock() {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement("TRUNCATE TABLE " + tableName)) {
            statement.executeUpdate();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private ObjectLock createLock(String tableName) {
        var name = TABLE_1.equals(tableName) ? null : tableName;
        return new ObjectLockBuilder()
                .waitTime(250, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(name)
                .build();
    }

}
