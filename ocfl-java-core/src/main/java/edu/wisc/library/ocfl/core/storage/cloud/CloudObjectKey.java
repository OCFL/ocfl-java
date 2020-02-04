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

package edu.wisc.library.ocfl.core.storage.cloud;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.util.Objects;

/**
 * Represents a key to an object in cloud storage.
 */
public class CloudObjectKey {

    private String prefix;
    private String path;
    private String key;

    public static Builder builder() {
        return new Builder();
    }

    /**
     * @param prefix the common prefix for an OCFL repository within a bucket, may be empty
     * @param path the part of an object key under the common prefix
     */
    private CloudObjectKey(String prefix, String path) {
        this.prefix = Enforce.notNull(prefix, "prefix cannot be null");
        this.path = Enforce.notNull(path, "path cannot be null");
        if (prefix.isBlank()) {
            this.key = this.path;
        } else {
            this.key = FileUtil.pathJoinIgnoreEmpty(prefix, path);
        }
    }

    /**
     * @param prefix the common prefix for an OCFL repository within a bucket, may be empty
     * @param path the part of an object key under the common prefix
     * @param key the composed object that includes the prefix and the path
     */
    private CloudObjectKey(String prefix, String path, String key) {
        this.prefix = Enforce.notNull(prefix, "prefix cannot be null");
        this.path = Enforce.notNull(path, "path cannot be null");
        this.key = Enforce.notBlank(key, "key cannot be blank");
    }

    /**
     * The common key prefix for an OCFL repository within a bucket, may be empty
     *
     * @return common key prefix for an OCFL repository within a bucket, may be empty
     */
    public String getPrefix() {
        return prefix;
    }

    /**
     * The part of the object key that's under the common prefix
     *
     * @return part of the object key that's under the common prefix
     */
    public String getPath() {
        return path;
    }

    /**
     * The composed object key that includes the prefix and the path
     *
     * @return composed object key that includes the prefix and the path
     */
    public String getKey() {
        return key;
    }

    @Override
    public String toString() {
        return key;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        CloudObjectKey that = (CloudObjectKey) o;
        return key.equals(that.key);
    }

    @Override
    public int hashCode() {
        return Objects.hash(key);
    }

    public static class Builder {

        private String prefix;

        public Builder() {
            this.prefix = "";
        }

        public Builder prefix(String prefix) {
            if (prefix == null) {
                this.prefix = "";
            } else if (prefix.endsWith("/")) {
                this.prefix = prefix.substring(0, indexLastNonSlash(prefix));
            } else {
                this.prefix = prefix;
            }
            return this;
        }

        private int indexLastNonSlash(String string) {
            for (int i = string.length(); i > 0; i--) {
                if (string.charAt(i - 1) != '/') {
                    return i;
                }
            }
            return 0;
        }

        public CloudObjectKey buildFromPath(String path) {
            return new CloudObjectKey(prefix, path);
        }

        public CloudObjectKey buildFromKey(String key) {
            Enforce.notBlank(key, "key cannot be blank");

            if (prefix.isBlank()) {
                return new CloudObjectKey(prefix, key, key);
            }

            return new CloudObjectKey(prefix, key.substring(prefix.length() + 1), key);
        }

    }

}
