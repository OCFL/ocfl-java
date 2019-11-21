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
                futures.forEach(future -> {
                    future.cancel(true);
                });
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
                futures.forEach(future -> {
                    future.cancel(true);
                });
                throw (RuntimeException) e.getCause();
            }
        }

        return results;
    }

}
