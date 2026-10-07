/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.util;

import io.ocfl.api.util.Enforce;
import io.ocfl.core.model.Version;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;
import java.util.function.BiFunction;

/**
 * Read-only map of logical paths to values derived from the state of a single version. Values are created on demand,
 * so looking up a single path does not require mapping every file in the version, which matters for objects that
 * contain a large number of files. Iterating over the entries or values creates a value for every file each time;
 * iterating over the keys does not create any values.
 *
 * @param <V> the type of value the paths are mapped to
 */
public class VersionStateMap<V> extends AbstractMap<String, V> {

    private final Version version;
    private final BiFunction<String, String, V> valueMapper;

    /**
     * @param version the version whose state is mapped
     * @param valueMapper creates the value for a logical path, given the path and its fileId
     */
    public VersionStateMap(Version version, BiFunction<String, String, V> valueMapper) {
        this.version = Enforce.notNull(version, "version cannot be null");
        this.valueMapper = Enforce.notNull(valueMapper, "valueMapper cannot be null");
    }

    @Override
    public V get(Object key) {
        if (!(key instanceof String)) {
            return null;
        }
        var path = (String) key;
        var fileId = version.getFileId(path);
        return fileId == null ? null : valueMapper.apply(path, fileId);
    }

    @Override
    public boolean containsKey(Object key) {
        return key instanceof String && version.getFileId((String) key) != null;
    }

    @Override
    public int size() {
        return version.getState().values().stream().mapToInt(Set::size).sum();
    }

    /**
     * Returns the logical paths in the version without creating any values.
     *
     * @return read-only set of logical paths
     */
    @Override
    public Set<String> keySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<String> iterator() {
                return version.getState().values().stream().flatMap(Set::stream).iterator();
            }

            @Override
            public boolean contains(Object o) {
                return containsKey(o);
            }

            @Override
            public int size() {
                return VersionStateMap.this.size();
            }
        };
    }

    @Override
    public Set<Entry<String, V>> entrySet() {
        return new AbstractSet<>() {
            @Override
            public Iterator<Entry<String, V>> iterator() {
                return version.getState().entrySet().stream()
                        .flatMap(entry -> entry.getValue().stream().map(path -> (Entry<String, V>)
                                new SimpleImmutableEntry<>(path, valueMapper.apply(path, entry.getKey()))))
                        .iterator();
            }

            @Override
            public int size() {
                return VersionStateMap.this.size();
            }
        };
    }
}
