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

package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Map;

/**
 * Represents a file within an OCFL object at a specific version. The file content can be lazy-loaded.
 */
public class OcflObjectVersionFile {

    private FileDetails fileDetails;
    private OcflFileRetriever fileRetriever;

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

    @Override
    public String toString() {
        return "OcflObjectVersionFile{" +
                "fileDetails='" + fileDetails + '\'' +
                '}';
    }

}
