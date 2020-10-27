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

import java.time.Instant;
import java.util.Objects;

/**
 * Encapsulates the results of a head operation
 */
public class HeadResult {

    private Long contentLength;
    private String contentEncoding;
    private String eTag;
    private Instant lastModified;

    public Long getContentLength() {
        return contentLength;
    }

    public HeadResult setContentLength(Long contentLength) {
        this.contentLength = contentLength;
        return this;
    }

    public String getContentEncoding() {
        return contentEncoding;
    }

    public HeadResult setContentEncoding(String contentEncoding) {
        this.contentEncoding = contentEncoding;
        return this;
    }

    public String getETag() {
        return eTag;
    }

    public HeadResult setETag(String eTag) {
        this.eTag = eTag;
        return this;
    }

    public Instant getLastModified() {
        return lastModified;
    }

    public HeadResult setLastModified(Instant lastModified) {
        this.lastModified = lastModified;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HeadResult that = (HeadResult) o;
        return Objects.equals(contentLength, that.contentLength) &&
                Objects.equals(contentEncoding, that.contentEncoding) &&
                Objects.equals(eTag, that.eTag) &&
                Objects.equals(lastModified, that.lastModified);
    }

    @Override
    public int hashCode() {
        return Objects.hash(contentLength, contentEncoding, eTag, lastModified);
    }

    @Override
    public String toString() {
        return "HeadResult{" +
                "contentLength=" + contentLength +
                ", contentEncoding='" + contentEncoding + '\'' +
                ", eTag='" + eTag + '\'' +
                ", lastModified=" + lastModified +
                '}';
    }

}
