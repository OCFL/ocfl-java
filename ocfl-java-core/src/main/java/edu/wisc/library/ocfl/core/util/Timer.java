package edu.wisc.library.ocfl.core.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.Callable;

public final class Timer {

    private static final Logger LOG = LoggerFactory.getLogger(Timer.class);

    public static void time(String name, Runnable runnable) {
        var start = Instant.now();
        try {
            runnable.run();
        } finally {
            var stop = Instant.now();
            LOG.debug("Timing {}: {}", name, Duration.between(start, stop).toMillis());
        }
    }

    public static <T> T time(String name, Callable<T> callable) {
        var start = Instant.now();
        try {
            return callable.call();
        } catch (Exception e) {
            if (e instanceof RuntimeException) {
                throw (RuntimeException) e;
            }
            throw new RuntimeException(e);
        } finally {
            var stop = Instant.now();
            LOG.debug("Timing {}: {}", name, Duration.between(start, stop).toMillis());
        }
    }

}
