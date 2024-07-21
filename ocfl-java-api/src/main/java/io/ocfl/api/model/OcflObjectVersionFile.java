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

import io.ocfl.api.OcflFileRetriever;
import io.ocfl.api.io.FixityCheckInputStream;
import io.ocfl.api.util.Enforce;
import java.io.InputStream;
import java.util.Map;

/**
 * Represents a file within an OCFL object at a specific version. The file content can be lazy-loaded.
 */
public class OcflObjectVersionFile {

    private final FileDetails fileDetails;
    private final OcflFileRetriever fileRetriever;

    public OcflObjectVersionFile(FileDetails fileDetails, OcflFileRetriever fileRetriever) {
        this.fileDetails = Enforce.notNull(fileDetails, "fileDetails cannot be null");
        this.fileRetriever = Enforce.notNull(fileRetriever, "fileRetriever cannot be null");
    }

    /**
     * The file's logical path within the object
     *
     * @return logical path
     */
    public String getPath() {
        return fileDetails.getPath();
    }

    /**
     * The file's path relative to the storage root
     *
     * @return storage relative path
     */
    public String getStorageRelativePath() {
        return fileDetails.getStorageRelativePath();
    }

    /**
     * Map of digest algorithm to digest value.
     *
     * @return digest map
     */
    public Map<DigestAlgorithm, String> getFixity() {
        return fileDetails.getFixity();
    }

    /**
     * Returns a new input stream of the file's content. The caller is responsible for closing the stream.
     *
     * <p>The caller may call {@code checkFixity()} on the InputStream after streaming all of that data to ensure the
     * fixity of data.
     *
     * @return FixityCheckInputStream of the file's content
     */
    public FixityCheckInputStream getStream() {
        return fileRetriever.retrieveFile();
    }

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
    public InputStream getRange(Long startPosition, Long endPosition) {
        return fileRetriever.retrieveRange(startPosition, endPosition);
    }

    @Override
    public String toString() {
        return "OcflObjectVersionFile{" + "fileDetails='" + fileDetails + '\'' + '}';
    }
}
