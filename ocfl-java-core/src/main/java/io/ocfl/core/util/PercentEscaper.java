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

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.annotations.Beta;
import com.google.common.annotations.GwtCompatible;
import com.google.common.escape.UnicodeEscaper;
import com.google.common.net.UrlEscapers;
import java.util.HashSet;
import java.util.Set;

/**
 * NOTICE: This class is copied from Guava 28.2-jre and modified to support lowercase hex encoding and either encoding
 * all characters not in a given set or encoding only the characters in a given set by Peter Winckles.
 *
 * <p>A {@code UnicodeEscaper} that escapes some set of Java characters using a UTF-8 based percent
 * encoding scheme. The set of safe characters (those which remain unescaped) can be specified on
 * construction.
 *
 * <p>This class is primarily used for creating URI escapers in {@link UrlEscapers} but can be used
 * directly if required. While URI escapers impose specific semantics on which characters are
 * considered 'safe', this class has a minimal set of restrictions.
 *
 * <p>When escaping a String, the following rules apply:
 *
 * <ul>
 *   <li>All specified safe characters remain unchanged.
 *   <li>If {@code plusForSpace} was specified, the space character " " is converted into a plus
 *       sign {@code "+"}.
 *   <li>All other characters are converted into one or more bytes using UTF-8 encoding and each
 *       byte is then represented by the 3-character string "%XX", where "XX" is the two-digit,
 *       uppercase, hexadecimal representation of the byte value.
 * </ul>
 *
 * <p>For performance reasons the only currently supported character encoding of this class is
 * UTF-8.
 *
 * <p><b>Note:</b> This escaper produces <a
 * href="https://url.spec.whatwg.org/#percent-encode">uppercase</a> hexadecimal sequences.
 *
 * @author David Beaumont
 * @since 15.0
 */
@Beta
@GwtCompatible
public final class PercentEscaper extends UnicodeEscaper {

    // In some escapers spaces are escaped to '+'
    private static final char[] PLUS_SIGN = {'+'};

    // Percent escapers output upper case hex digits (uri escapers require this).
    private static final char[] UPPER_HEX_DIGITS = "0123456789ABCDEF".toCharArray();

    // Modification 02/03/2020
    private static final char[] LOWER_HEX_DIGITS = "0123456789abcdef".toCharArray();

    private final Behavior behavior;

    /** If true we should convert space to the {@code +} character. */
    private final boolean plusForSpace;

    /**
     * An array of flags where for any {@code char c} if {@code safeOctets[c]} is true then {@code c}
     * should remain unmodified in the output. If {@code c >= safeOctets.length} then it should be
     * escaped.
     */
    private final boolean[] octets;

    // Modification added 02/03/2020
    private final char[] hexDigits;

    /**
     * Creates a builder without any safe or unsafe characters set.
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder(false);
    }

    /**
     * Creates a builder with safe alpha numeric characters preconfigured.
     *
     * @return builder
     */
    public static Builder builderWithSafeAlphaNumeric() {
        return new Builder(true);
    }

    // Builder added 02/03/2020
    public static class Builder {
        private static final String ALPHA_NUMERIC_CHARS =
                "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";

        private boolean plusForSpace = false;
        private boolean useUppercase = false;

        private Behavior behavior = null;
        private final Set<Character> charSet = new HashSet<>();

        private Builder(boolean safeAlphaNumeric) {
            if (safeAlphaNumeric) {
                behavior = Behavior.ENCODE_NOT_IN_SET;
                addToSet(ALPHA_NUMERIC_CHARS, charSet);
            }
        }

        private static void addToSet(String chars, Set<Character> charSet) {
            chars.chars().forEach(c -> charSet.add((char) c));
        }

        private static void addRangeToSet(char start, char end, Set<Character> charSet) {
            checkArgument(start < end, "The start char must come before the end char.");
            for (var c = start; c <= end; c++) {
                charSet.add(c);
            }
        }

        /**
         * Adds a range of characters to the set of characters that do not need to be encoded.
         *
         * @param start the first safe character in the range
         * @param end the last safe character in the range
         * @return this
         */
        public Builder addSafeCharRange(char start, char end) {
            if (behavior == Behavior.ENCODE_SET) {
                throw new IllegalArgumentException(
                        "Cannot add safe characters because the escaper is already configured with unsafe characters.");
            }
            behavior = Behavior.ENCODE_NOT_IN_SET;
            addRangeToSet(start, end, charSet);
            return this;
        }

        /**
         * Adds each character in the string to the set of characters that do not need to be encoded.
         *
         * @param safeChars characters that do not need to be encoded
         * @return this
         */
        public Builder addSafeChars(String safeChars) {
            if (behavior == Behavior.ENCODE_SET) {
                throw new IllegalArgumentException(
                        "Cannot add safe characters because the escaper is already configured with unsafe characters.");
            }
            behavior = Behavior.ENCODE_NOT_IN_SET;
            addToSet(safeChars, charSet);
            return this;
        }

        /**
         * Adds a range of characters to the set of characters that need to be encoded.
         *
         * @param start the first unsafe character in the range
         * @param end the last unsafe character in the range
         * @return this
         */
        public Builder addUnsafeCharRange(char start, char end) {
            if (behavior == Behavior.ENCODE_NOT_IN_SET) {
                throw new IllegalArgumentException(
                        "Cannot add unsafe characters because the escaper is already configured with safe characters.");
            }
            behavior = Behavior.ENCODE_SET;
            addRangeToSet(start, end, charSet);
            return this;
        }

        /**
         * Adds each character in the string to the set of characters that need to be encoded.
         *
         * @param unsafeChars characters that need to be encoded
         * @return this
         */
        public Builder addUnsafeChars(String unsafeChars) {
            if (behavior == Behavior.ENCODE_NOT_IN_SET) {
                throw new IllegalArgumentException(
                        "Cannot add unsafe characters because the escaper is already configured with safe characters.");
            }
            behavior = Behavior.ENCODE_SET;
            addToSet(unsafeChars, charSet);
            return this;
        }

        /**
         * @param plusForSpace true if ASCII space should be escaped to {@code +} rather than {@code %20}
         * @return this
         */
        public Builder plusForSpace(boolean plusForSpace) {
            this.plusForSpace = plusForSpace;
            return this;
        }

        /**
         * @param useUppercase true if hex characters should be upper case
         * @return this
         */
        public Builder useUppercase(boolean useUppercase) {
            this.useUppercase = useUppercase;
            return this;
        }

        public PercentEscaper build() {
            return new PercentEscaper(behavior, charSet, plusForSpace, useUppercase);
        }
    }

    private enum Behavior {
        ENCODE_SET,
        ENCODE_NOT_IN_SET
    }

    /**
     * Constructs a percent escaper with the specified safe characters and optional handling of the
     * space character.
     *
     * <p>Not that it is allowed, but not necessarily desirable to specify {@code %} as a safe
     * character. This has the effect of creating an escaper which has no well defined inverse but it
     * can be useful when escaping additional characters.
     *
     * @param behavior defines whether every character not in the charSet should be encoded
     *                or only characters in the charSet should be encoded
     * @param charSet the set of characters that should or should not be encoded
     * @param plusForSpace true if ASCII space should be escaped to {@code +} rather than {@code %20}
     * @param useUppercase true if hex characters should be upper case
     * @throws IllegalArgumentException if any of the parameters were invalid
     */
    private PercentEscaper(Behavior behavior, Set<Character> charSet, boolean plusForSpace, boolean useUppercase) {
        checkNotNull(charSet); // eager for GWT.

        this.behavior = checkNotNull(behavior);
        this.plusForSpace = plusForSpace;

        var charSetCopy = new HashSet<>(charSet);

        if (behavior == Behavior.ENCODE_SET) {
            if (plusForSpace) {
                charSetCopy.add(' ');
            }
            charSetCopy.add('%');
        } else {
            if (plusForSpace) {
                charSetCopy.remove(' ');
            }
            charSetCopy.remove('%');
        }

        this.octets = createOctets(charSetCopy);

        // Following lines added on 02/03/2020
        if (useUppercase) {
            hexDigits = UPPER_HEX_DIGITS;
        } else {
            hexDigits = LOWER_HEX_DIGITS;
        }
    }

    /**
     * Creates a boolean array with entries corresponding to the character values specified in
     * charSet set to true. The array is as small as is required to hold the given character
     * information.
     */
    private static boolean[] createOctets(Set<Character> charSet) {
        int maxChar = -1;
        for (char c : charSet) {
            maxChar = Math.max(c, maxChar);
        }
        boolean[] octets = new boolean[maxChar + 1];
        for (char c : charSet) {
            octets[c] = true;
        }
        return octets;
    }

    /*
     * Overridden for performance. For unescaped strings this improved the performance of the uri
     * escaper from ~760ns to ~400ns as measured by {@link CharEscapersBenchmark}.
     */
    @Override
    protected int nextEscapeIndex(CharSequence csq, int index, int end) {
        checkNotNull(csq);
        for (; index < end; index++) {
            char c = csq.charAt(index);
            if (shouldEscapeChar(c)) {
                break;
            }
        }
        return index;
    }

    /*
     * Overridden for performance. For unescaped strings this improved the performance of the uri
     * escaper from ~400ns to ~170ns as measured by {@link CharEscapersBenchmark}.
     */
    @Override
    public String escape(String s) {
        checkNotNull(s);
        int slen = s.length();
        for (int index = 0; index < slen; index++) {
            char c = s.charAt(index);
            if (shouldEscapeChar(c)) {
                return escapeSlow(s, index);
            }
        }
        return s;
    }

    private boolean shouldEscapeChar(char c) {
        return (behavior == Behavior.ENCODE_NOT_IN_SET && (c >= octets.length || !octets[c]))
                || (behavior == Behavior.ENCODE_SET && (c < octets.length && octets[c]));
    }

    /** Escapes the given Unicode code point in UTF-8. */
    @Override
    protected char[] escape(int cp) {
        // We should never get negative values here but if we do it will throw an
        // IndexOutOfBoundsException, so at least it will get spotted.
        if (!shouldEscapeChar((char) cp)) {
            return null;
        } else if (cp == ' ' && plusForSpace) {
            return PLUS_SIGN;
        } else if (cp <= 0x7F) {
            // Single byte UTF-8 characters
            // Start with "%--" and fill in the blanks
            char[] dest = new char[3];
            dest[0] = '%';
            dest[2] = hexDigits[cp & 0xF];
            dest[1] = hexDigits[cp >>> 4];
            return dest;
        } else if (cp <= 0x7ff) {
            // Two byte UTF-8 characters [cp >= 0x80 && cp <= 0x7ff]
            // Start with "%--%--" and fill in the blanks
            char[] dest = new char[6];
            dest[0] = '%';
            dest[3] = '%';
            dest[5] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[4] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[2] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[1] = hexDigits[0xC | cp];
            return dest;
        } else if (cp <= 0xffff) {
            // Three byte UTF-8 characters [cp >= 0x800 && cp <= 0xffff]
            // Start with "%E-%--%--" and fill in the blanks
            char[] dest = new char[9];
            dest[0] = '%';
            dest[1] = 'E';
            dest[3] = '%';
            dest[6] = '%';
            dest[8] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[7] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[5] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[4] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[2] = hexDigits[cp];
            return dest;
        } else if (cp <= 0x10ffff) {
            char[] dest = new char[12];
            // Four byte UTF-8 characters [cp >= 0xffff && cp <= 0x10ffff]
            // Start with "%F-%--%--%--" and fill in the blanks
            dest[0] = '%';
            dest[1] = 'F';
            dest[3] = '%';
            dest[6] = '%';
            dest[9] = '%';
            dest[11] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[10] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[8] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[7] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[5] = hexDigits[cp & 0xF];
            cp >>>= 4;
            dest[4] = hexDigits[0x8 | (cp & 0x3)];
            cp >>>= 2;
            dest[2] = hexDigits[cp & 0x7];
            return dest;
        } else {
            // If this ever happens it is due to bug in UnicodeEscaper, not bad input.
            throw new IllegalArgumentException("Invalid unicode character value " + cp);
        }
    }
}
