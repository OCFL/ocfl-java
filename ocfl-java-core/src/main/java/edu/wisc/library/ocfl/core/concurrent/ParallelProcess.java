package edu.wisc.library.ocfl.core.concurrent;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Consumer;
import java.util.function.Function;

public class ParallelProcess {

    private ExecutorService executor;

    public ParallelProcess(ExecutorService executor) {
        this.executor = Enforce.notNull(executor, "executor cannot be null");
    }

    public <T> void collection(Collection<T> collection, Consumer<T> task) {
        var completionService = new CancellableCompletionService<Void>(executor);

        collection.forEach(entry -> {
            completionService.submit(() -> {
                task.accept(entry);
            });
        });

        completionService.awaitAllCancelOnException();
    }

    public <T, U> void map(Map<T, U> map, BiConsumer<T, U> task) {
        var completionService = new CancellableCompletionService<Void>(executor);

        map.forEach((k, v) -> {
            completionService.submit(() -> {
                task.accept(k, v);
            });
        });

        completionService.awaitAllCancelOnException();
    }

    public <T, R> List<R> collection(Collection<T> collection, Function<T, R> task) {
        var completionService = new CancellableCompletionService<R>(executor);

        collection.forEach(entry -> {
            completionService.submit(() -> {
                return task.apply(entry);
            });
        });

        return completionService.mergeAllCancelOnException();
    }

    public <T, U, R> List<R> map(Map<T, U> map, BiFunction<T, U, R> task) {
        var completionService = new CancellableCompletionService<R>(executor);

        map.forEach((k, v) -> {
            completionService.submit(() -> {
                return task.apply(k, v);
            });
        });

        return completionService.mergeAllCancelOnException();
    }

    public void shutdown() {
        ExecutorTerminator.shutdown(executor);
    }

}
