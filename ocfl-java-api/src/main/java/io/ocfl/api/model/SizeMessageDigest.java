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

import java.security.MessageDigest;
import java.util.concurrent.atomic.AtomicLong;

/**
* Defines a digest algorithms which calculates the size (byte count) for the input
* and returns it as integer represented as string as specified in
* OCFL community extension 9: https://ocfl.github.io/extensions/0009-digest-algorithms
*/
public class SizeMessageDigest extends MessageDigest {

    public SizeMessageDigest() {
        super("size");
    }

    AtomicLong size = new AtomicLong(0);

    @Override
    protected void engineUpdate(byte input) {
        size.incrementAndGet();
    }

    @Override
    protected void engineUpdate(byte[] input, int offset, int len) {
        size.addAndGet(len);
    }

    @Override
    protected byte[] engineDigest() {
        return SizeMessageDigest.formatDigestAsByteArray(size.get());
    }

    @Override
    protected void engineReset() {
        size.set(0);
    }

    /** 
     * convert byte array of message digest into a hex compatible String
     * add leading zero to ensure that the length of string is even 
     * 
     * @param digest as byte[]
     * @return the String containing the decimal value of the size
     */
    public static String formatDigest(byte[] digest) {
        //Java17: String sizeResult = java.util.HexFormat.of().formatHex(digest);
        StringBuilder result = new StringBuilder();
        for (byte aByte : digest) {
            result.append(String.format("%02x", aByte));
        }
        return result.length() % 2 == 0 ? result.toString() : "0" + result.toString();
    }

    /** 
     * convert the digest value into a hex compatible String
     * add leading zero to ensure that the length of string is even 
     * 
     * @param size as long value
     * @return the String containing the decimal value of the size
     */
    public static String formatDigest(long size) {
        String sizeResult = Long.toString(size);
        return sizeResult.length() % 2 == 0 ? sizeResult : "0" + sizeResult;
    }

    /** 
     * convert the digest value into a byte array
     * 
     * @param size as long value
     * @return the byte array representing the size 
     *         as a string representation of the integer in decimal notation
     */
    public static byte[] formatDigestAsByteArray(long size) {
        String s = formatDigest(size);
        byte[] result = new byte[s.length() / 2];
        for (int i = 0; i < result.length; i++) {
            int index = i * 2;
            int val = Integer.parseInt(s.substring(index, index + 2), 16);
            result[i] = (byte) val;
        }
        return result;
    }

}
