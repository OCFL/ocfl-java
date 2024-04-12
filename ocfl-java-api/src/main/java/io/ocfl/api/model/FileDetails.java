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

package io.ocfl.api.model;

import io.ocfl.api.util.Enforce;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Encapsulates a filePath with all of its fixity information.
 */
public class FileDetails {

    private String path;
    private String storageRelativePath;
    private Map<DigestAlgorithm, String> fixity;

    public FileDetails() {
        this.fixity = new HashMap<>();
    }

    /**
     * The file's logical path within the object
     *
     * @return logical path
     */
    public String getPath() {
        return path;
    }

    public FileDetails setPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * The file's path relative to the storage root
     *
     * @return storage relative path
     */
    public String getStorageRelativePath() {
        return storageRelativePath;
    }

    public FileDetails setStorageRelativePath(String storageRelativePath) {
        this.storageRelativePath = storageRelativePath;
        return this;
    }

    /**
     * Map of digest algorithm to digest value.
     *
     * @return digest map
     */
    public Map<DigestAlgorithm, String> getFixity() {
        return fixity;
    }

    public FileDetails setFixity(Map<DigestAlgorithm, String> fixity) {
        this.fixity = fixity;
        return this;
    }

    public FileDetails addDigest(DigestAlgorithm algorithm, String value) {
        Enforce.notNull(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be null");
        this.fixity.put(algorithm, value);
        return this;
    }

    @Override
    public String toString() {
        return "FileDetails{" + "path='"
                + path + '\'' + "storageRelativePath='"
                + storageRelativePath + '\'' + ", fixity="
                + fixity + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        FileDetails that = (FileDetails) o;
        return Objects.equals(path, that.path)
                && Objects.equals(storageRelativePath, that.storageRelativePath)
                && Objects.equals(fixity, that.fixity);
    }

    @Override
    public int hashCode() {
        return Objects.hash(path, storageRelativePath, fixity);
    }
}
