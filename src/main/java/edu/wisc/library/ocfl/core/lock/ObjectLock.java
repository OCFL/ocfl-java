package edu.wisc.library.ocfl.core.lock;

import java.util.concurrent.Callable;

public interface ObjectLock {

    void doInLock(String objectId, Runnable doInLock);

    <T> T doInLock(String objectId, Callable<T> doInLock);

}
