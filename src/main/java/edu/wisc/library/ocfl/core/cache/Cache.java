package edu.wisc.library.ocfl.core.cache;

import java.util.function.Function;

public interface Cache<K,V> {

    V get(K key, Function<K, V> loader);

    void put(K key, V value);

    void invalidate(K key);

}
