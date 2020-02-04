/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
     * @param key key
     * @param value value
     */
    void put(K key, V value);

    /**
     * Invalidates the key in the cache.
     *
     * @param key key
     */
    void invalidate(K key);

}
