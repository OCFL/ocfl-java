package edu.wisc.library.ocfl.core.cache;

import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.LoadingCache;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.function.Function;

public class CaffeineCache<K, V> implements Cache<K, V> {

    private Caffeine<K, V> builder;
    private LoadingCache<K, V> cache;

    public CaffeineCache(Caffeine caffeine) {
        // Caffeine's generics are a little off...
        builder = Enforce.notNull(caffeine, "caffeine cannot be null");
    }

    @Override
    public synchronized void initialize(Function<K, V> loader) {
        if (cache != null) {
            throw new IllegalStateException("The Caffeine cache has already been initialized.");
        }
        Enforce.notNull(loader, "loader cannot be null");
        cache = builder.build(loader::apply);
    }

    @Override
    public V get(K key) {
        return cache.get(key);
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
