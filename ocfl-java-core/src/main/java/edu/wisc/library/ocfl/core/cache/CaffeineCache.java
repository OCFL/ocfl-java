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

    /**
     * {@inheritDoc}
     */
    @Override
    public V get(K key, Function<K, V> loader) {
        return cache.get(key, loader);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void put(K key, V value) {
        cache.put(key, value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidate(K key) {
        cache.invalidate(key);
    }

}
