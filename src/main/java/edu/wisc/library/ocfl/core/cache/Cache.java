package edu.wisc.library.ocfl.core.cache;

import java.util.function.Function;

public interface Cache<K,V> {

    // TODO this is potentially a bad idea
    void initialize(Function<K, V> loader);

    V get(K key);

    void put(K key, V value);

    void invalidate(K key);

}
