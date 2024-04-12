/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.ocfl.core.lock;

import java.util.concurrent.Callable;

/**
 * Extension point that allows the OCFL repository to use any number of different lock implementations so long as they
 * conform to this interface.
 *
 * @see InMemoryObjectLock
 */
public interface ObjectLock {

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
