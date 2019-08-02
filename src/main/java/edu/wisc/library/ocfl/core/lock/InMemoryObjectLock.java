package edu.wisc.library.ocfl.core.lock;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public class InMemoryObjectLock implements ObjectLock {

    private Map<String, Lock> locks;
    private long waitTime;
    private TimeUnit timeUnit;

    public InMemoryObjectLock(long waitTime, TimeUnit timeUnit) {
        this.locks = new ConcurrentHashMap<>();
        this.waitTime = Enforce.expressionTrue(waitTime >= 0, waitTime, "waitTime must be at least 0");
        this.timeUnit = Enforce.notNull(timeUnit, "timeUnit cannot be null");
    }

    @Override
    public void doInLock(String objectId, Runnable doInLock) {
        var lock = locks.computeIfAbsent(objectId, k -> new ReentrantLock());

        try {
            if (lock.tryLock(waitTime, timeUnit)) {
                try {
                    doInLock.run();
                } finally {
                    lock.unlock();
                }
            } else {
                // TODO modeled exception
                throw new RuntimeException("Failed to acquire lock for object " + objectId);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T doInLock(String objectId, Callable<T> doInLock) {
        var lock = locks.computeIfAbsent(objectId, k -> new ReentrantLock());

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
                // TODO modeled exception
                throw new RuntimeException("Failed to acquire lock for object " + objectId);
            }
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
