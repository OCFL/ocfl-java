package edu.wisc.library.ocfl.core.lock;

import java.util.concurrent.Callable;

public interface ObjectLock {

    void doInReadLock(String objectId, Runnable doInLock);

    <T> T doInReadLock(String objectId, Callable<T> doInLock);

    void doInWriteLock(String objectId, Runnable doInLock);

    <T> T doInWriteLock(String objectId, Callable<T> doInLock);

}
