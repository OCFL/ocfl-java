package edu.wisc.library.ocfl.core.concurrent;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

public final class ExecutorTerminator {

    private static final Logger LOG = LoggerFactory.getLogger(ExecutorTerminator.class);

    private ExecutorTerminator() {

    }

    public static ExecutorService addShutdownHook(ExecutorService executor) {
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            executor.shutdown();
            try {
                if (!executor.awaitTermination(30, TimeUnit.SECONDS)) {
                    LOG.warn("Executor did not shutdown in allotted time. Forcing shutdown.");
                    executor.shutdownNow();
                }
            } catch (InterruptedException e) {
                LOG.warn("Interrupted shutting down executor", e);
            }
        }));
        return executor;
    }

}
