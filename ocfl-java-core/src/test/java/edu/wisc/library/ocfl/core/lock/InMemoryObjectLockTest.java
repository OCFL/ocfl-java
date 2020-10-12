package edu.wisc.library.ocfl.core.lock;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.exception.LockException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

public class InMemoryObjectLockTest {

    private InMemoryObjectLock lock;
    private Cache<String, ReentrantLock> cache;
    private ExecutorService executor;

    @BeforeEach
    public void setup() {
        cache = Caffeine.newBuilder().weakValues().build();
        lock = new InMemoryObjectLock(cache, 2, TimeUnit.SECONDS);
        executor = Executors.newFixedThreadPool(2);
    }

    @Test
    public void shouldRemoveValuesWhenNoLongerReferenced() throws Exception {
        var id = "obj1";

        var future = executor.submit(() -> {
            lock.doInWriteLock(id, () -> {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        TimeUnit.MILLISECONDS.sleep(100);

        assertTrue(cache.asMap().containsKey(id));

        future.get();

        // This is non-deterministic...
        System.gc();

        TimeUnit.MILLISECONDS.sleep(100);

        assertFalse(cache.asMap().containsKey(id));
    }

    @Test
    public void shouldBlockWhenLockAlreadyHeld() throws Exception {
        var id = "obj1";

        executor.submit(() -> {
            lock.doInWriteLock(id, () -> {
                try {
                    TimeUnit.SECONDS.sleep(3);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        });

        TimeUnit.MILLISECONDS.sleep(100);

        assertThrows(LockException.class, () -> {
            lock.doInWriteLock(id, () -> {
                fail("Should not have acquired lock");
            });
        });
    }

}
