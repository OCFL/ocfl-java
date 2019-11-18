package edu.wisc.library.ocfl.core.lock;

import java.util.concurrent.Callable;

/**
 * Extension point that allows the OCFL repository to use any number of different lock implementations so long as they
 * conform to this interface.
 *
 * @see InMemoryObjectLock
 */
public interface ObjectLock {

    /**
     * Executes the code block after securing a read lock on the objectId. The lock is released after the block completes.
     *
     * @param objectId id of the object
     * @param doInLock block to execute within the lock
     */
    void doInReadLock(String objectId, Runnable doInLock);

    /**
     * Executes the code block after securing a read lock on the objectId. The lock is released after the block completes.
     *
     * @param objectId id of the object
     * @param doInLock block to execute within the lock
     * @param <T> return type
     * @return object
     */
    <T> T doInReadLock(String objectId, Callable<T> doInLock);

    /**
     * Executes the code block after securing a write lock on the objectId. The lock is released after the block completes.
     *
     * @param objectId id of the object
     * @param doInLock block to execute within the lock
     */
    void doInWriteLock(String objectId, Runnable doInLock);

    /**
     * Executes the code block after securing a write lock on the objectId. The lock is released after the block completes.
     *
     * @param objectId id of the object
     * @param doInLock block to execute within the lock
     * @param <T> return type
     * @return object
     */
    <T> T doInWriteLock(String objectId, Callable<T> doInLock);

}
