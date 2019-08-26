package edu.wisc.library.ocfl.core.cache;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.function.Function;

/**
 * In-memory cache implementation that is a wrapper around a Caffeine cache.
 *
 * @see <a href="https://github.com/ben-manes/caffeine">Caffeine</a>
 */
public class CaffeineCache<K, V> implements Cache<K, V> {

    private com.github.benmanes.caffeine.cache.Cache<K, V> cache;

    public CaffeineCache(com.github.benmanes.caffeine.cache.Cache cache) {
        this.cache = Enforce.notNull(cache, "cache cannot be null");
    }

    @Override
    public V get(K key, Function<K, V> loader) {
        return cache.get(key, loader);
    }

    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

}
