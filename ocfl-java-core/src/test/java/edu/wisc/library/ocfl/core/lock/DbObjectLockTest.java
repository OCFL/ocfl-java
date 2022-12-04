package edu.wisc.library.ocfl.core.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import edu.wisc.library.ocfl.api.exception.LockException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class DbObjectLockTest {

    private static ComboPooledDataSource dataSource;

    private ExecutorService executor;
    private ObjectLock lock;

    @BeforeAll
    public static void beforeAll() {
        dataSource = new ComboPooledDataSource();
        dataSource.setJdbcUrl(System.getProperty("db.url", "jdbc:h2:mem:test"));
        dataSource.setUser(System.getProperty("db.user", ""));
        dataSource.setPassword(System.getProperty("db.password", ""));
    }

    @BeforeEach
    public void setup() {
        lock = createLock(Duration.ofHours(1));
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

    @Test
    public void onConcurrentAcquireOnlyOneProcessShouldGetLock() throws InterruptedException {
        var phaser = new Phaser(3);

        var result1 = new AtomicBoolean(false);
        var future1 = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            lock.doInWriteLock("obj1", () -> {
                result1.set(true);
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        var result2 = new AtomicBoolean(false);
        var future2 = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            lock.doInWriteLock("obj1", () -> {
                result2.set(true);
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        try {
            future1.get();
            future2.get();
        } catch (ExecutionException e) {
            // don't care about this
        }

        assertFalse(result1.get() && result2.get());
        assertTrue(result1.get() || result2.get());
    }

    @Test
    public void shouldAcquireLockWhenExistsButIsExpired() throws ExecutionException, InterruptedException {
        lock = createLock(Duration.ofMillis(100));

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

        Thread.sleep(TimeUnit.MILLISECONDS.toMillis(101));

        var result1 = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result1.set(true);
        });
        assertTrue(result1.get());

        var result2 = new AtomicBoolean(false);
        lock.doInWriteLock("obj1", () -> {
            result2.set(true);
        });
        assertTrue(result2.get());

        future.get();
    }

    @Test
    public void onConcurrentAcquireOnlyOneProcessShouldGetLockWhenLockExpired()
            throws ExecutionException, InterruptedException {
        lock = createLock(Duration.ofMillis(100));

        var phaser = new Phaser(4);

        var future0 = executor.submit(() -> {
            lock.doInWriteLock("obj1", () -> {
                phaser.arriveAndAwaitAdvance();
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(500));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        var result1 = new AtomicBoolean(false);
        var future1 = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(101));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            lock.doInWriteLock("obj1", () -> {
                result1.set(true);
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        var result2 = new AtomicBoolean(false);
        var future2 = executor.submit(() -> {
            phaser.arriveAndAwaitAdvance();
            try {
                Thread.sleep(TimeUnit.MILLISECONDS.toMillis(101));
            } catch (InterruptedException e) {
                Thread.interrupted();
            }
            lock.doInWriteLock("obj1", () -> {
                result2.set(true);
                try {
                    Thread.sleep(TimeUnit.MILLISECONDS.toMillis(100));
                } catch (InterruptedException e) {
                    Thread.interrupted();
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        try {
            future0.get();
            future1.get();
            future2.get();
        } catch (ExecutionException e) {
            // don't care about this
        }

        assertFalse(result1.get() && result2.get());
        assertTrue(result1.get() || result2.get());
    }

    private ObjectLock createLock(Duration maxLockDuration) {
        var tableName = "lock_" + UUID.randomUUID().toString().replaceAll("-", "");
        return new ObjectLockBuilder()
                .dataSource(dataSource)
                .tableName(tableName)
                .maxLockDuration(maxLockDuration)
                .build();
    }
}
