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

package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.BitSet;

/**
 * Constraint that applies restrictions on what characters are allowed in file names.
 */
public class BitSetPathCharConstraint implements PathCharConstraint {

    private BitSet charSet;
    private boolean blackList;

    /**
     * Creates a constraint that rejects the specified characters
     *
     * @param chars the characters to reject
     * @return constraint
     */
    public static BitSetPathCharConstraint blackList(char... chars) {
        return build(true, chars);
    }

    /**
     * Creates a constraint that rejects all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint blackListRange(char start, char end) {
        return build(true, start, end);
    }

    /**
     * Creates a constraint that only accepts the specified characters
     *
     * @param chars the characters to accept
     * @return constraint
     */
    public static BitSetPathCharConstraint whiteList(char... chars) {
        return build(false, chars);
    }

    /**
     * Creates a constraint that accepts all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint whiteListRange(char start, char end) {
        return build(false, start, end);
    }

    private static BitSetPathCharConstraint build(boolean blackList, char... chars) {
        var charSet = new BitSet(256);
        for (var c : chars) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blackList, charSet);
    }

    private static BitSetPathCharConstraint build(boolean blackList, char start, char end) {
        var charSet = new BitSet(256);
        for (var c = start; c <= end; c++) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blackList, charSet);
    }

    public BitSetPathCharConstraint(boolean blackList, BitSet charSet) {
        this.blackList = blackList;
        this.charSet = Enforce.notNull(charSet, "charSet cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(char c, String path) {
        if (charSet.get(c)) {
            if (blackList) {
                throwException(c, path);
            }
        } else if (!blackList) {
            throwException(c, path);
        }
    }

    private void throwException(char c, String path) {
        throw new PathConstraintException(String.format("The path contains the illegal character '%s'. Path: %s", c, path));
    }

}
