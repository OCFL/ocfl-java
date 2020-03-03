/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
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

package edu.wisc.library.ocfl.core.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

public class CancellableCompletionService<T> extends ExecutorCompletionService<T> {

    private static final Logger LOG = LoggerFactory.getLogger(CancellableCompletionService.class);

    private List<Future<T>> futures;

    public CancellableCompletionService(Executor executor) {
        super(executor);
        futures = new ArrayList<>();
    }

    @Override
    public Future<T> submit(Callable<T> task) {
        var future = super.submit(task);
        futures.add(future);
        return future;
    }

    public Future<T> submit(Runnable task) {
        return submit(task, null);
    }

    @Override
    public Future<T> submit(Runnable task, T result) {
        var future = super.submit(task, result);
        futures.add(future);
        return future;
    }

    public void awaitAllCancelOnException() {
        for (var i = 0; i < futures.size(); i++) {
            try {
                take().get();
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                // TODO rethrowing the cause loses the stack trace
                LOG.info("Exception in processing thread", e);
                cancelFutures();
                throw (RuntimeException) e.getCause();
            }
        }
    }

    public List<T> mergeAllCancelOnException() {
        var results = new ArrayList<T>(futures.size());

        for (var i = 0; i < futures.size(); i++) {
            try {
                results.add(take().get());
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            } catch (ExecutionException e) {
                // TODO rethrowing the cause loses the stack trace
                LOG.info("Exception in processing thread", e);
                cancelFutures();
                throw (RuntimeException) e.getCause();
            }
        }

        return results;
    }

    private void cancelFutures() {
        try {
            futures.forEach(future -> future.cancel(true));
            // Wait for all of the futures to complete. This is slower, but will avoid some bugs.
            futures.forEach(future -> {
                try {
                    future.get();
                } catch (Exception e) {
                    // Ignore
                }
            });
        } catch (RuntimeException e) {
            // Ignore
        }
    }

}
