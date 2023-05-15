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

package io.ocfl.core.path.constraint;

import io.ocfl.api.exception.PathConstraintException;
import io.ocfl.api.util.Enforce;
import java.util.BitSet;

/**
 * Constraint that applies restrictions on what characters are allowed in file names.
 */
public class BitSetPathCharConstraint implements PathCharConstraint {

    private final BitSet charSet;
    private final boolean blockList;

    /**
     * Creates a constraint that rejects the specified characters
     *
     * @param chars the characters to reject
     * @return constraint
     */
    public static BitSetPathCharConstraint blockList(char... chars) {
        return build(true, chars);
    }

    /**
     * Creates a constraint that rejects all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint blockListRange(char start, char end) {
        return build(true, start, end);
    }

    /**
     * Creates a constraint that only accepts the specified characters
     *
     * @param chars the characters to accept
     * @return constraint
     */
    public static BitSetPathCharConstraint acceptList(char... chars) {
        return build(false, chars);
    }

    /**
     * Creates a constraint that accepts all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint acceptListRange(char start, char end) {
        return build(false, start, end);
    }

    private static BitSetPathCharConstraint build(boolean blockList, char... chars) {
        var charSet = new BitSet(256);
        for (var c : chars) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blockList, charSet);
    }

    private static BitSetPathCharConstraint build(boolean blockList, char start, char end) {
        Enforce.expressionTrue(start < end, start, "The start char must come before the end char.");
        var charSet = new BitSet(256);
        for (var c = start; c <= end; c++) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blockList, charSet);
    }

    public BitSetPathCharConstraint(boolean blockList, BitSet charSet) {
        this.blockList = blockList;
        this.charSet = Enforce.notNull(charSet, "charSet cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(char c, String path) {
        if (charSet.get(c)) {
            if (blockList) {
                throwException(c, path);
            }
        } else if (!blockList) {
            throwException(c, path);
        }
    }

    private void throwException(char c, String path) {
        throw new PathConstraintException(
                String.format("The path contains the illegal character '%s'. Path: %s", c, path));
    }
}
