package edu.wisc.library.ocfl.core.lock;

public interface ObjectLock {

    void doInLock(String objectId, Runnable doInLock);

}
