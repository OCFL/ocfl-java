package edu.wisc.library.ocfl.core.cache;

import java.util.function.Function;

/**
 * Pass-through cache implementation that never caches.
 */
public class NoOpCache<K, V> implements Cache<K, V> {

    @Override
    public V get(K key, Function<K, V> loader) {
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
