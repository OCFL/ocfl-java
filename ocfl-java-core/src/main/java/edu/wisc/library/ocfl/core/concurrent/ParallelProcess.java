package edu.wisc.library.ocfl.core.concurrent;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.ArrayList;
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

    /**
     * Executes the task on each element of the collection in parallel. If there's only one element in the collection,
     * then it is executed immediately in the current thread. The method blocks until all of the tasks complete. If one
     * of the tasks fails, then all of the outstanding tasks are cancelled.
     *
     * @param collection the collection of objects to feed to the task
     * @param task the task to execute on each element of the collection
     * @param <T> the type of element in the collection
     */
    public <T> void collection(Collection<T> collection, Consumer<T> task) {
        if (collection.size() == 1) {
            task.accept(collection.iterator().next());
            return;
        }

        var completionService = new CancellableCompletionService<Void>(executor);

        collection.forEach(entry -> {
            completionService.submit(() -> {
                task.accept(entry);
            });
        });

        completionService.awaitAllCancelOnException();
    }

    /**
     * Executes the task on each element of the map in parallel. If there's only one element in the map, then it is executed
     * immediately in the current thread. The method blocks until all of the tasks complete. If one of the tasks fails,
     * then all of the outstanding tasks are cancelled.
     *
     * @param map the map of objects to feed to the task
     * @param task the task to execute on each element of the map
     * @param <T> the type of key in the map
     * @param <U> the type of value in the map
     */
    public <T, U> void map(Map<T, U> map, BiConsumer<T, U> task) {
        if (map.size() == 1) {
            var entry = map.entrySet().iterator().next();
            task.accept(entry.getKey(), entry.getValue());
            return;
        }

        var completionService = new CancellableCompletionService<Void>(executor);

        map.forEach((k, v) -> {
            completionService.submit(() -> {
                task.accept(k, v);
            });
        });

        completionService.awaitAllCancelOnException();
    }

    /**
     * Executes the task on each element of the collection in parallel. If there's only one element in the collection,
     * then it is executed immediately in the current thread. The method blocks until all of the tasks complete. If one
     * of the tasks fails, then all of the outstanding tasks are cancelled. The results of the tasks are collected and returned in a list.
     *
     * @param collection the collection of objects to feed to the task
     * @param task the task to execute on each element of the collection
     * @param <T> the type of element in the collection
     * @param <R> the return type
     */
    public <T, R> List<R> collection(Collection<T> collection, Function<T, R> task) {
        if (collection.size() == 1) {
            var list = new ArrayList<R>();
            list.add(task.apply(collection.iterator().next()));
            return list;
        }

        var completionService = new CancellableCompletionService<R>(executor);

        collection.forEach(entry -> {
            completionService.submit(() -> {
                return task.apply(entry);
            });
        });

        return completionService.mergeAllCancelOnException();
    }

    /**
     * Executes the task on each element of the map in parallel. If there's only one element in the map, then it is executed
     * immediately in the current thread. The method blocks until all of the tasks complete. If one of the tasks fails,
     * then all of the outstanding tasks are cancelled. The results of the tasks are collected and returned in a list.
     *
     * @param map the map of objects to feed to the task
     * @param task the task to execute on each element of the map
     * @param <T> the type of key in the map
     * @param <U> the type of value in the map
     * @param <R> the return type
     */
    public <T, U, R> List<R> map(Map<T, U> map, BiFunction<T, U, R> task) {
        if (map.size() == 1) {
            var entry = map.entrySet().iterator().next();
            var list = new ArrayList<R>();
            list.add(task.apply(entry.getKey(), entry.getValue()));
            return list;
        }

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
