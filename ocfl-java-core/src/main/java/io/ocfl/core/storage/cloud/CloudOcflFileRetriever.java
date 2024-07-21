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

package io.ocfl.core.storage.cloud;

import io.ocfl.api.OcflFileRetriever;
import io.ocfl.api.io.FixityCheckInputStream;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.util.Enforce;
import java.io.BufferedInputStream;
import java.io.InputStream;

/**
 * OcflFileRetriever implementation for lazy-loading files from cloud storage.
 */
public class CloudOcflFileRetriever implements OcflFileRetriever {

    private final CloudClient cloudClient;
    private final String key;
    private final DigestAlgorithm digestAlgorithm;
    private final String digestValue;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private CloudClient cloudClient;

        public Builder cloudClient(CloudClient cloudClient) {
            this.cloudClient = cloudClient;
            return this;
        }

        public CloudOcflFileRetriever build(String key, DigestAlgorithm digestAlgorithm, String digestValue) {
            return new CloudOcflFileRetriever(cloudClient, key, digestAlgorithm, digestValue);
        }
    }

    public CloudOcflFileRetriever(
            CloudClient cloudClient, String key, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
        this.key = Enforce.notBlank(key, "key cannot be blank");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.digestValue = Enforce.notBlank(digestValue, "digestValue cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile() {
        return new FixityCheckInputStream(
                new BufferedInputStream(cloudClient.downloadStream(key)), digestAlgorithm, digestValue);
    }

    /**
     * Returns an input stream of the file's content between the specified byte range. startPosition and endPosition
     * may be null. When they are null, they are translated into an empty string. startPosition and endPosition are
     * used to construct byte range as specified in <a link="https://www.rfc-editor.org/rfc/rfc9110.html#name-byte-ranges">RFC 9110</a>.
     *
     * <p>The caller is responsible for closing the stream. The input stream is buffered.
     *
     * @param startPosition the byte offset in the file to start reading, inclusive
     * @param endPosition the byte offset in the file to stop reading, inclusive
     * @return a buffered input stream containing the specified file data
     */
    @Override
    public InputStream retrieveRange(Long startPosition, Long endPosition) {
        var start = startPosition == null ? "" : startPosition;
        var end = endPosition == null ? "" : endPosition;
        var range = "bytes=" + start + "-" + end;
        return new BufferedInputStream(cloudClient.downloadStreamRange(key, range));
    }
}
