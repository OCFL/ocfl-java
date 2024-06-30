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
import java.nio.ByteBuffer;
import java.security.MessageDigest;

/**
 * Defines a digest algorithms which calculates the size (byte count) for the input
 * and returns it as integer represented as string as specified in
 * <a href="https://ocfl.github.io/extensions/0009-digest-algorithms">OCFL community extension 9</a>
 */
public class SizeDigestAlgorithm extends DigestAlgorithm {

    public SizeDigestAlgorithm() {
        super("size", "size");
    }

    @Override
    public MessageDigest getMessageDigest() {
        return new SizeMessageDigest();
    }

    @Override
    public String encode(byte[] value) {
        return String.valueOf(Bytes.wrap(value).toLong());
    }

    private static class SizeMessageDigest extends MessageDigest {

        public SizeMessageDigest() {
            super("size");
        }

        private long size = 0;

        @Override
        protected void engineUpdate(byte input) {
            size++;
        }

        @Override
        protected void engineUpdate(byte[] input, int offset, int len) {
            size += len;
        }

        @Override
        protected byte[] engineDigest() {
            var buffer = ByteBuffer.allocate(Long.BYTES);
            buffer.putLong(size);
            return buffer.array();
        }

        @Override
        protected void engineReset() {
            size = 0;
        }
    }
}
