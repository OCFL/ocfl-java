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

package io.ocfl.core.storage.common;

import io.ocfl.api.util.Enforce;
import java.util.Objects;

/**
 * The result of a storage list operation
 */
public class Listing {

    public enum Type {
        Directory,
        File,
        Other,
    }

    private final Type type;
    private final String relativePath;

    /**
     * Creates a file listing. The path MUST use forward slashes as path separators.
     *
     * @param relativePath relative path to the file
     * @return file listing
     */
    public static Listing file(String relativePath) {
        return new Listing(Type.File, relativePath);
    }

    /**
     * Creates a directory listing. The path MUST use forward slashes as path separators.
     *
     * @param relativePath relative path to the directory
     * @return directory listing
     */
    public static Listing directory(String relativePath) {
        return new Listing(Type.Directory, relativePath);
    }

    /**
     * Creates a listing for a file that is neither a regular file nor directory, such as a symbolic link.
     * The path MUST use forward slashes as path separators.
     *
     * @param relativePath relative path to the file
     * @return file listing
     */
    public static Listing other(String relativePath) {
        return new Listing(Type.Other, relativePath);
    }

    public Listing(Type type, String relativePath) {
        this.type = Enforce.notNull(type, "type cannot be null");
        this.relativePath = Enforce.notNull(relativePath, "relativePath cannot be null");
    }

    /**
     * @return the path to the file/directory relative the list operation path
     */
    public String getRelativePath() {
        return relativePath;
    }

    /**
     * @return the type of file
     */
    public Type getType() {
        return type;
    }

    public boolean isFile() {
        return type == Type.File;
    }

    public boolean isDirectory() {
        return type == Type.Directory;
    }

    public boolean isOther() {
        return type == Type.Other;
    }

    @Override
    public String toString() {
        return "Listing{" + "type=" + type + ", relativePath='" + relativePath + '\'' + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Listing listing = (Listing) o;
        return type == listing.type && relativePath.equals(listing.relativePath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(type, relativePath);
    }
}
