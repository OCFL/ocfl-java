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

package io.ocfl.core.util;

import at.favre.lib.bytes.Bytes;
import io.ocfl.api.exception.OcflIOException;
import io.ocfl.api.model.DigestAlgorithm;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.security.MessageDigest;

public final class DigestUtil {

    private static final int BUFFER_SIZE = 8192;

    private DigestUtil() {}

    public static String computeDigestHex(DigestAlgorithm algorithm, Path path) {
        return computeDigestHex(algorithm.getMessageDigest(), path);
    }

    public static String computeDigestHex(MessageDigest digest, Path path) {
        return computeDigestHex(digest, path, false);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, Path path, boolean upperCase) {
        return computeDigestHex(algorithm.getMessageDigest(), path, upperCase);
    }

    public static String computeDigestHex(MessageDigest digest, Path path, boolean upperCase) {
        return Bytes.wrap(computeDigest(digest, path)).encodeHex(upperCase);
    }

    public static byte[] computeDigest(DigestAlgorithm algorithm, Path path) {
        return computeDigest(algorithm.getMessageDigest(), path);
    }

    public static byte[] computeDigest(MessageDigest digest, Path path) {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

            while (channel.read(buffer) > -1) {
                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            return digest.digest();
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    public static byte[] computeDigest(DigestAlgorithm algorithm, ByteBuffer buffer) {
        var digest = algorithm.getMessageDigest();
        digest.update(buffer);
        buffer.flip();
        return digest.digest();
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, String value) {
        return computeDigestHex(algorithm, value, false);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, String value, boolean upperCase) {
        return Bytes.from(value).hash(algorithm.getJavaStandardName()).encodeHex(upperCase);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, byte[] value) {
        return computeDigestHex(algorithm, value, false);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, byte[] value, boolean upperCase) {
        return Bytes.wrap(value).hash(algorithm.getJavaStandardName()).encodeHex(upperCase);
    }
}
