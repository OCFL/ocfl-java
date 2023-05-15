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

package io.ocfl.core.storage.common;

import io.ocfl.api.OcflConstants;
import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that iterates over OCFL object root directories. Object roots are identified by the presence of a file that's
 * prefixed with '0=ocfl_object'.
 */
public abstract class OcflObjectRootDirIterator implements Iterator<String>, Closeable {

    private boolean started = false;
    private boolean closed = false;

    private final ArrayDeque<Directory> dirStack;
    private String next;

    public OcflObjectRootDirIterator() {
        this.dirStack = new ArrayDeque<>();
    }

    /**
     * Indicates if a directory path is an object root path
     *
     * @param path directory path
     * @return true if path is an object root path
     */
    protected abstract boolean isObjectRoot(String path);

    /**
     * Creates an object to maintain directory state
     *
     * @param path directory path
     * @return directory object
     */
    protected abstract Directory createDirectory(String path);

    @Override
    public void close() {
        if (!closed) {
            while (!dirStack.isEmpty()) {
                popDirectory();
            }
            closed = true;
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed.");
        }
        fetchNextIfNeeded();
        return next != null;
    }

    @Override
    public String next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more files found.");
        }
        var result = next;
        next = null;
        return result;
    }

    private void fetchNextIfNeeded() {
        if (next == null) {
            var nextDirectory = fetchNextDirectory();

            while (nextDirectory != null) {
                if (shouldSkip(nextDirectory)) {
                    popDirectory();
                } else if (isObjectRoot(nextDirectory)) {
                    // Do not process children
                    popDirectory();
                    next = nextDirectory;
                    return;
                }

                nextDirectory = fetchNextDirectory();
            }
        }
    }

    private String fetchNextDirectory() {
        if (!started) {
            dirStack.push(createDirectory(""));
            started = true;
        }

        var top = dirStack.peek();

        while (top != null) {
            var child = top.nextChildDirectory();

            if (child == null) {
                popDirectory();
                top = dirStack.peek();
            } else {
                dirStack.push(createDirectory(child));
                return child;
            }
        }

        return null;
    }

    private void popDirectory() {
        if (!dirStack.isEmpty()) {
            try {
                dirStack.pop().close();
            } catch (IOException e) {
                // ignore
            }
        }
    }

    private boolean shouldSkip(String path) {
        return OcflConstants.EXTENSIONS_DIR.equals(path);
    }

    /**
     * Encapsulates a directory for iterating over its children
     */
    protected interface Directory extends Closeable {
        /**
         * Returns the path to the directory's next child relative the storage root. Forward slashes MUST be used
         * as path separators. If the directory has no more children, then null is returned.
         *
         * @return path to next child directory or null
         */
        String nextChildDirectory();
    }
}
