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

package io.ocfl.core.storage.filesystem;

import io.ocfl.api.OcflFileRetriever;
import io.ocfl.api.exception.OcflIOException;
import io.ocfl.api.io.FixityCheckInputStream;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.util.Enforce;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * OcflFileRetriever implementation for lazy-loading files from file system storage.
 */
public class FileSystemOcflFileRetriever implements OcflFileRetriever {

    private final Path filePath;
    private final DigestAlgorithm digestAlgorithm;
    private final String digestValue;

    public FileSystemOcflFileRetriever(Path filePath, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.filePath = Enforce.notNull(filePath, "filePath cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.digestValue = Enforce.notBlank(digestValue, "digestValue cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile() {
        try {
            return new FixityCheckInputStream(
                    new BufferedInputStream(Files.newInputStream(filePath)), digestAlgorithm, digestValue);
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    /**
     * Returns an input stream of the file's content between the specified byte range. startPosition and endPosition
     * may not be null.
     *
     * <p>The caller is responsible for closing the stream. The input stream is buffered.
     *
     * @param startPosition the byte offset in the file to start reading, inclusive
     * @param endPosition the byte offset in the file to stop reading, inclusive
     * @return a buffered input stream containing the specified file data
     */
    @Override
    public InputStream retrieveRange(Long startPosition, Long endPosition) {
        try {
            var length = endPosition - startPosition;
            var file = new RandomAccessFile(filePath.toFile(), "r");
            if (startPosition > 0) {
                file.seek(startPosition);
            }
            return new InputStream() {
                long bytesRead = 0;

                @Override
                public int read() throws IOException {
                    if (bytesRead > length) {
                        return -1;
                    }
                    bytesRead++;
                    return file.read();
                }

                @Override
                public void close() throws IOException {
                    file.close();
                }
            };
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }
}
