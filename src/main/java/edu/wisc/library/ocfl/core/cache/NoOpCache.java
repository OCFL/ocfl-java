package edu.wisc.library.ocfl.core.cache;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.function.Function;

public class NoOpCache<K, V> implements Cache<K, V> {

    private Function<K, V> loader;

    @Override
    public void initialize(Function<K, V> loader) {
        if (this.loader != null) {
            throw new IllegalStateException("The NoOp cache has already been initialized.");
        }
        this.loader = Enforce.notNull(loader, "loader cannot be null");
    }

    @Override
    public V get(K key) {
        return loader.apply(key);
    }

    @Override
    public void put(K key, V value) {
        // no op
    }

    @Override
    public void invalidate(K key) {
        // no op
    }

}
