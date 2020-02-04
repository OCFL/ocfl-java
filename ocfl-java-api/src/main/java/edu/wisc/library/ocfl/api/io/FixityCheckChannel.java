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

package edu.wisc.library.ocfl.api.io;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class FixityCheckChannel implements ByteChannel {

    private boolean enabled = true;

    private final ByteChannel delegate;
    private final MessageDigest digest;
    private final String expectedDigestValue;

    public FixityCheckChannel(ByteChannel delegate, DigestAlgorithm digestAlgorithm, String expectedDigestValue) {
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
        this.digest = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null").getMessageDigest();
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

    public FixityCheckChannel(ByteChannel delegate, String digestAlgorithm, String expectedDigestValue) {
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
        this.digest = messageDigest(Enforce.notBlank(digestAlgorithm, "digestAlgorithm cannot be blank"));
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

    public void checkFixity() {
        if (enabled) {
            var actualDigest = Bytes.wrap(digest.digest()).encodeHex();
            if (!expectedDigestValue.equalsIgnoreCase(actualDigest)) {
                throw new FixityCheckException(String.format("Expected %s digest: %s; Actual: %s",
                        digest.getAlgorithm(), expectedDigestValue, actualDigest));
            }
        }

    }

    public String getExpectedDigestValue() {
        return expectedDigestValue;
    }

    public FixityCheckChannel enableFixityCheck(boolean enabled) {
        this.enabled = enabled;
        return this;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        var r = delegate.read(dst);
        if (enabled && r > 0) {
            dst.flip();
            digest.update(dst);
        }
        return r;
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        if (enabled) {
            digest.update(src);
            src.flip();
        }
        return delegate.write(src);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    private static MessageDigest messageDigest(String digestAlgorithm) {
        try {
            return MessageDigest.getInstance(digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
