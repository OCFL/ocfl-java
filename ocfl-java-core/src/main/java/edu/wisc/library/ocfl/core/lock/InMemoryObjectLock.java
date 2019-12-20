package edu.wisc.library.ocfl.core.lock;

import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * In-memory implementation of ObjectLock that uses Java's ReentrantReadWriteLock.
 */
public class InMemoryObjectLock implements ObjectLock {

    private Map<String, ReentrantReadWriteLock> locks;
    private long waitTime;
    private TimeUnit timeUnit;

    /**
     * How long to wait when attempting to acquire a lock.
     *
     * @param waitTime how long to wait for the lock
     * @param timeUnit unit of wait time
     */
    public InMemoryObjectLock(long waitTime, TimeUnit timeUnit) {
        this.locks = new ConcurrentHashMap<>();
        this.waitTime = Enforce.expressionTrue(waitTime >= 0, waitTime, "waitTime must be at least 0");
        this.timeUnit = Enforce.notNull(timeUnit, "timeUnit cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doInReadLock(String objectId, Runnable doInLock) {
        doInReadLock(objectId, () -> {
            doInLock.run();
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T doInReadLock(String objectId, Callable<T> doInLock) {
        var lock = locks.computeIfAbsent(objectId, k -> new ReentrantReadWriteLock()).readLock();
        return doInLock(objectId, lock, doInLock);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doInWriteLock(String objectId, Runnable doInLock) {
        doInWriteLock(objectId, () -> {
            doInLock.run();
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T doInWriteLock(String objectId, Callable<T> doInLock) {
        var lock = locks.computeIfAbsent(objectId, k -> new ReentrantReadWriteLock()).writeLock();
        return doInLock(objectId, lock, doInLock);
    }

    private <T> T doInLock(String objectId, Lock lock, Callable<T> doInLock) {
        try {
            if (lock.tryLock(waitTime, timeUnit)) {
                try {
                    return doInLock.call();
                } catch (RuntimeException e) {
                    throw e;
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    lock.unlock();
                }
            } else {
                throw new LockException("Failed to acquire lock for object " + objectId);
            }
        } catch (InterruptedException e) {
            throw new LockException(e);
        }
    }

}
