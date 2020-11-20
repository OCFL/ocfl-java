package edu.wisc.library.ocfl.core.lock;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.exception.LockException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.UUID;
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

    private String tableName;
    private ComboPooledDataSource dataSource;
    private ExecutorService executor;
    private ObjectLock lock;

    @BeforeEach
    public void setup() {
        tableName = "lock_" + UUID.randomUUID().toString().replaceAll("-", "");
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl("jdbc:h2:mem:test");

        lock = createLock(tableName);
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void after() {
        executor.shutdown();
    }

    @Test
    public void shouldAcquireLockWhenDoesNotExist() {
        var result = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result.set(true);
        });
        assertTrue(result.get());
    }

    @Test
    public void shouldAcquireLockWhenAlreadyExistsButNotHeld() {
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

    @Test
    public void shouldThrowExceptionWhenCannotAcquireLock() throws ExecutionException, InterruptedException {
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

    private ObjectLock createLock(String tableName) {
        return new ObjectLockBuilder()
                .waitTime(250, TimeUnit.MILLISECONDS)
                .dataSource(dataSource)
                .tableName(tableName)
                .build();
    }

}
