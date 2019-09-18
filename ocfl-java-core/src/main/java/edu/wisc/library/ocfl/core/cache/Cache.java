package edu.wisc.library.ocfl.core.cache;

import java.util.function.Function;

/**
 * Extension point that allows the OCFL repository to use any number of different cache implementations so long as they
 * conform to this interface.
 *
 * @see CaffeineCache
 *
 * @param <K> type of cache key
 * @param <V> type of cache value
 */
public interface Cache<K,V> {

    /**
     * Retrieves a value from the cache. If it doesn't exist, the loader is called to load the object, which is then
     * cached and returned.
     *
     * @param key to lookup in the cache
     * @param loader function to call to load the object if it's not found
     * @return the object that maps to the key
     */
    V get(K key, Function<K, V> loader);

    /**
     * Inserts a value into the cache.
     *
     * @param key
     * @param value
     */
    void put(K key, V value);

    /**
     * Invalidates the key in the cache.
     *
     * @param key
     */
    void invalidate(K key);

}
