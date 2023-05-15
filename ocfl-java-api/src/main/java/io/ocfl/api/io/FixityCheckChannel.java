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

package io.ocfl.api.io;

import at.favre.lib.bytes.Bytes;
import io.ocfl.api.exception.FixityCheckException;
import io.ocfl.api.exception.OcflJavaException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.util.Enforce;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ByteChannel;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * A byte channel wrapper that preforms a fixity check on the bytes that pass through an underlying byte channel.
 * After all of the channel's bytes have been read, {@code checkFixity()} should be called.
 */
public class FixityCheckChannel implements ByteChannel {

    private boolean enabled = true;

    private final ByteChannel delegate;
    private final MessageDigest digest;
    private final String expectedDigestValue;

    /**
     * Constructs a new FixityCheckChannel
     *
     * @param delegate the channel to wrap
     * @param digestAlgorithm the digest algorithm to use
     * @param expectedDigestValue the expected digest value
     */
    public FixityCheckChannel(ByteChannel delegate, DigestAlgorithm digestAlgorithm, String expectedDigestValue) {
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
        this.digest = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null")
                .getMessageDigest();
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

    /**
     * Constructs a new FixityCheckChannel
     *
     * @param delegate the channel to wrap
     * @param digestAlgorithm the digest algorithm to use
     * @param expectedDigestValue the expected digest value
     */
    public FixityCheckChannel(ByteChannel delegate, String digestAlgorithm, String expectedDigestValue) {
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
        this.digest = messageDigest(Enforce.notBlank(digestAlgorithm, "digestAlgorithm cannot be blank"));
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

    /**
     * Performs a fixity check and throws an exception if the check fails. This should only be called after the entire
     * contents of the stream has been read.
     *
     * <p>If the check is disabled, nothing happens
     *
     * @throws FixityCheckException when the actual digest value does not match the expected value
     */
    public void checkFixity() {
        if (enabled) {
            var actualDigest = Bytes.wrap(digest.digest()).encodeHex();
            if (!expectedDigestValue.equalsIgnoreCase(actualDigest)) {
                throw new FixityCheckException(String.format(
                        "Expected %s digest: %s; Actual: %s",
                        digest.getAlgorithm(), expectedDigestValue, actualDigest));
            }
        }
    }

    /**
     * @return the expected digest value
     */
    public String getExpectedDigestValue() {
        return expectedDigestValue;
    }

    /**
     * By default fixity checking is enabled. Use this method to disable it, and prevent needless digest computation
     *
     * @param enabled if fixity should be checked
     * @return this stream
     */
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
            throw new OcflJavaException(e);
        }
    }
}
