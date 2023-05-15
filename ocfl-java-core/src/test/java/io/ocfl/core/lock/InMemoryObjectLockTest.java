package io.ocfl.core.lock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import io.ocfl.api.exception.LockException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InMemoryObjectLockTest {

    private InMemoryObjectLock lock;
    private Cache<String, ReentrantLock> cache;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        cache = Caffeine.newBuilder().weakValues().build();
        lock = new InMemoryObjectLock(cache, 250, TimeUnit.MILLISECONDS);
        executor = Executors.newCachedThreadPool();
    }

    @AfterEach
    public void after() {
        executor.shutdown();
    }

    @Test
    public void shouldRemoveValuesWhenNoLongerReferenced() throws Exception {
        var id = "obj1";

        var phaser = new Phaser(2);

        var future = executor.submit(() -> {
            lock.doInWriteLock(id, () -> {
                phaser.arriveAndAwaitAdvance();
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        assertTrue(cache.asMap().containsKey(id));

        future.get();

        // This is non-deterministic...
        System.gc();

        TimeUnit.MILLISECONDS.sleep(100);

        assertFalse(cache.asMap().containsKey(id));
    }

    @Test
    public void shouldBlockWhenLockAlreadyHeld() {
        var id = "obj1";

        var phaser = new Phaser(2);

        executor.submit(() -> {
            lock.doInWriteLock(id, () -> {
                phaser.arriveAndAwaitAdvance();
                try {
                    TimeUnit.MILLISECONDS.sleep(500);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        phaser.arriveAndAwaitAdvance();

        assertThrows(LockException.class, () -> {
            lock.doInWriteLock(id, () -> {
                fail("Should not have acquired lock");
            });
        });
    }
}
