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

package edu.wisc.library.ocfl.core.util;

import com.google.common.annotations.Beta;
import com.google.common.annotations.GwtCompatible;
import com.google.common.escape.UnicodeEscaper;
import com.google.common.net.UrlEscapers;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * NOTICE: This class is copied from Guava 28.2-jre and modified to support lowercase hex encoding and additional replacement
 * mappings on 02/03/2020 by Peter Winckles. Additionally, it was modified to use other characters besides % to mark the
 * start of an encoding.
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

    /** If true we should convert space to the {@code +} character. */
    private final boolean plusForSpace;

    /**
     * An array of flags where for any {@code char c} if {@code safeOctets[c]} is true then {@code c}
     * should remain unmodified in the output. If {@code c >= safeOctets.length} then it should be
     * escaped.
     */
    private final boolean[] safeOctets;

    // Modification 02/03/2020
    private final char encodeStartChar;

    // Modification added 02/03/2020
    private final char[] hexDigits;

    // Modification added 02/03/2020
    private final char[][] replacements;
    // Modification added 02/03/2020
    private final int replacementsLength;

    // Builder added 02/03/2020
    public static class Builder {
        private char encodeStartChar;
        private String safeChars;
        private boolean plusForSpace;
        private boolean useUppercase;
        private Map<Character, String> additionalMappings;

        Builder() {
            this.encodeStartChar = '%';
            plusForSpace = true;
            useUppercase = true;
            additionalMappings = new HashMap<>();
        }

        public Builder encodeStartChar(char c) {
            this.encodeStartChar = c;
            return this;
        }

        public Builder safeChars(String safeChars) {
            this.safeChars = safeChars;
            return this;
        }

        public Builder plusForSpace(boolean plusForSpace) {
            this.plusForSpace = plusForSpace;
            return this;
        }

        public Builder useUppercase(boolean useUppercase) {
            this.useUppercase = useUppercase;
            return this;
        }

        public Builder addEscape(char c, String replacement) {
            this.additionalMappings.put(c, replacement);
            return this;
        }

        public PercentEscaper build() {
            return new PercentEscaper(safeChars, plusForSpace, useUppercase, additionalMappings, encodeStartChar);
        }
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * Constructs a percent escaper with the specified safe characters and optional handling of the
     * space character.
     *
     * <p>Not that it is allowed, but not necessarily desirable to specify {@code %} as a safe
     * character. This has the effect of creating an escaper which has no well defined inverse but it
     * can be useful when escaping additional characters.
     *
     * @param safeChars a non null string specifying additional safe characters for this escaper (the
     *     ranges 0..9, a..z and A..Z are always safe and should not be specified here)
     * @param plusForSpace true if ASCII space should be escaped to {@code +} rather than {@code %20}
     * @throws IllegalArgumentException if any of the parameters were invalid
     */
    public PercentEscaper(String safeChars, boolean plusForSpace) {
        this(safeChars, plusForSpace, true, Collections.emptyMap(), '%');
    }

    /**
     * Constructs a percent escaper with the specified safe characters and optional handling of the
     * space character.
     *
     * <p>Not that it is allowed, but not necessarily desirable to specify {@code %} as a safe
     * character. This has the effect of creating an escaper which has no well defined inverse but it
     * can be useful when escaping additional characters.
     *
     * @param safeChars a non null string specifying additional safe characters for this escaper (the
     *     ranges 0..9, a..z and A..Z are always safe and should not be specified here)
     * @param plusForSpace true if ASCII space should be escaped to {@code +} rather than {@code %20}
     * @param useUppercase true if hex characters should be upper case
     * @param replacementMap map of characters that should be replaced rather than percent encoded
     * @param encodeStartChar character to use to denote beginning of hex encoding, typically %
     * @throws IllegalArgumentException if any of the parameters were invalid
     */
    private PercentEscaper(String safeChars, boolean plusForSpace, boolean useUppercase, Map<Character, String> replacementMap, char encodeStartChar) {
        // TODO(dbeaumont): Switch to static factory methods for creation now that class is final.
        // TODO(dbeaumont): Support escapers where alphanumeric chars are not safe.
        checkNotNull(safeChars); // eager for GWT.
        // Avoid any misunderstandings about the behavior of this escaper
        if (safeChars.matches(".*[0-9A-Za-z].*")) {
            throw new IllegalArgumentException(
                    "Alphanumeric characters are always 'safe' and should not be explicitly specified");
        }
        safeChars += "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
        // Avoid ambiguous parameters. Safe characters are never modified so if
        // space is a safe character then setting plusForSpace is meaningless.
        if (plusForSpace && safeChars.contains(" ")) {
            throw new IllegalArgumentException(
                    "plusForSpace cannot be specified when space is a 'safe' character");
        }
        this.plusForSpace = plusForSpace;
        this.safeOctets = createSafeOctets(safeChars);

        // Following lines added on 02/03/2020
        if (useUppercase) {
            hexDigits = UPPER_HEX_DIGITS;
        } else {
            hexDigits = LOWER_HEX_DIGITS;
        }
        checkNotNull(replacementMap);
        this.replacements = createReplacementArray(replacementMap);
        this.replacementsLength = replacements.length;
        this.encodeStartChar = encodeStartChar;
    }

    // Modification 02/03/2020 -- copied from ArrayBasedCharEscaperMap
    private static final char[][] EMPTY_REPLACEMENT_ARRAY = new char[0][0];
    private static char[][] createReplacementArray(Map<Character, String> map) {
        if (map.isEmpty()) {
            return EMPTY_REPLACEMENT_ARRAY;
        }
        char max = Collections.max(map.keySet());
        char[][] replacements = new char[max + 1][];
        for (char c : map.keySet()) {
            replacements[c] = map.get(c).toCharArray();
        }
        return replacements;
    }

    /**
     * Creates a boolean array with entries corresponding to the character values specified in
     * safeChars set to true. The array is as small as is required to hold the given character
     * information.
     */
    private static boolean[] createSafeOctets(String safeChars) {
        int maxChar = -1;
        char[] safeCharArray = safeChars.toCharArray();
        for (char c : safeCharArray) {
            maxChar = Math.max(c, maxChar);
        }
        boolean[] octets = new boolean[maxChar + 1];
        for (char c : safeCharArray) {
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
            if (c >= safeOctets.length || !safeOctets[c]) {
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
            if (c >= safeOctets.length || !safeOctets[c]) {
                return escapeSlow(s, index);
            }
        }
        return s;
    }

    /** Escapes the given Unicode code point in UTF-8. */
    @Override
    protected char[] escape(int cp) {
        // We should never get negative values here but if we do it will throw an
        // IndexOutOfBoundsException, so at least it will get spotted.
        if (cp < safeOctets.length && safeOctets[cp]) {
            return null;
        } else if (cp == ' ' && plusForSpace) {
            return PLUS_SIGN;
        // Modification 02/03/2020
        } else if (cp < replacementsLength && replacements[cp] != null) {
            return replacements[cp];
        } else if (cp <= 0x7F) {
            // Single byte UTF-8 characters
            // Start with "%--" and fill in the blanks
            char[] dest = new char[3];
            dest[0] = encodeStartChar;
            dest[2] = hexDigits[cp & 0xF];
            dest[1] = hexDigits[cp >>> 4];
            return dest;
        } else if (cp <= 0x7ff) {
            // Two byte UTF-8 characters [cp >= 0x80 && cp <= 0x7ff]
            // Start with "%--%--" and fill in the blanks
            char[] dest = new char[6];
            dest[0] = encodeStartChar;
            dest[3] = encodeStartChar;
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
            dest[0] = encodeStartChar;
            dest[1] = 'E';
            dest[3] = encodeStartChar;
            dest[6] = encodeStartChar;
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
            dest[0] = encodeStartChar;
            dest[1] = 'F';
            dest[3] = encodeStartChar;
            dest[6] = encodeStartChar;
            dest[9] = encodeStartChar;
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

