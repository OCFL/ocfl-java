/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2024 OCFL Java Implementers Group
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

import at.favre.lib.bytes.Bytes;
import io.ocfl.api.exception.OcflJavaException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Implementation of {@link DigestAlgorithm} that uses a standard {@link MessageDigest} and encodes the digest value
 * to a lowercase, hex string.
 */
public class StandardDigestAlgorithm extends DigestAlgorithm {

    public StandardDigestAlgorithm(String ocflName, String javaStandardName) {
        super(ocflName, javaStandardName);
    }

    @Override
    public MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(getJavaStandardName());
        } catch (NoSuchAlgorithmException e) {
            throw new OcflJavaException("Failed to create message digest for: " + this, e);
        }
    }

    @Override
    public String encode(byte[] value) {
        return Bytes.wrap(value).encodeHex();
    }
}
