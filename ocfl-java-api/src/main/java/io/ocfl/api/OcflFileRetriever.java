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

package io.ocfl.api;

import io.ocfl.api.io.FixityCheckInputStream;
import java.io.InputStream;

/**
 * This class is used to lazy-load object files. A new instance should be created for each file that's intended to be load.
 */
public interface OcflFileRetriever {

    /**
     * Returns a new input stream of the file's content. The caller is responsible for closing the stream. The input
     * stream is buffered.
     *
     * <p>The caller may call {@link FixityCheckInputStream#checkFixity} on the InputStream after streaming all of that data to ensure the
     * fixity of data.
     *
     * @return FixityCheckInputStream of the file's content
     */
    FixityCheckInputStream retrieveFile();

    /**
     * Returns an input stream of the file's content between the specified byte range. startPosition and endPosition
     * may be null, depending on the underlying implementation, and the meaning of a null value is also implementation
     * dependent.
     *
     * <p>The caller is responsible for closing the stream. The input stream is buffered.
     *
     * @param startPosition the byte offset in the file to start reading, inclusive
     * @param endPosition the byte offset in the file to stop reading, inclusive
     * @return a buffered input stream containing the specified file data
     */
    InputStream retrieveRange(Long startPosition, Long endPosition);
}
