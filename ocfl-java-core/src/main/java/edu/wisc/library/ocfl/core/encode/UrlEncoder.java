package edu.wisc.library.ocfl.core.encode;

import java.io.CharArrayWriter;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * This code is copied from URLEncoder with one modification. It can now be configured to return uppercase or lowercase
 * hex values.
 *
 * @see java.net.URLEncoder
 */
public class UrlEncoder implements Encoder {

    private BitSet dontNeedEncoding;
    private final int caseDiff = ('a' - 'A');
    private boolean useUppercase;

    /**
     * @param useUppercase use upper or lower case in escape sequence
     */
    public UrlEncoder(boolean useUppercase) {
        this.useUppercase = useUppercase;
        dontNeedEncoding = new BitSet(256);
        int i;
        for (i = 'a'; i <= 'z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = 'A'; i <= 'Z'; i++) {
            dontNeedEncoding.set(i);
        }
        for (i = '0'; i <= '9'; i++) {
            dontNeedEncoding.set(i);
        }
        dontNeedEncoding.set(' '); /* encoding a space to a + is done
                                    * in the encode() method */
        dontNeedEncoding.set('-');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('.');
        dontNeedEncoding.set('*');
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encode(String input) {
        boolean needToChange = false;
        StringBuilder out = new StringBuilder(input.length());
        CharArrayWriter charArrayWriter = new CharArrayWriter();

        for (int i = 0; i < input.length();) {
            int c = input.charAt(i);
            if (dontNeedEncoding.get(c)) {
                if (c == ' ') {
                    c = '+';
                    needToChange = true;
                }
                out.append((char)c);
                i++;
            } else {
                // convert to external encoding before hex conversion
                do {
                    charArrayWriter.write(c);
                    /*
                     * If this character represents the start of a Unicode
                     * surrogate pair, then pass in two characters. It's not
                     * clear what should be done if a byte reserved in the
                     * surrogate pairs range occurs outside of a legal
                     * surrogate pair. For now, just treat it as if it were
                     * any other character.
                     */
                    if (c >= 0xD800 && c <= 0xDBFF) {
                        if ( (i+1) < input.length()) {
                            int d = input.charAt(i+1);
                            if (d >= 0xDC00 && d <= 0xDFFF) {
                                charArrayWriter.write(d);
                                i++;
                            }
                        }
                    }
                    i++;
                } while (i < input.length() && !dontNeedEncoding.get((c = input.charAt(i))));

                charArrayWriter.flush();
                String str = new String(charArrayWriter.toCharArray());
                byte[] ba = str.getBytes(StandardCharsets.UTF_8);
                for (int j = 0; j < ba.length; j++) {
                    out.append('%');
                    char ch = Character.forDigit((ba[j] >> 4) & 0xF, 16);
                    // converting to use uppercase letter as part of
                    // the hex value if ch is a letter.
                    if (useUppercase && Character.isLetter(ch)) {
                        ch -= caseDiff;
                    }
                    out.append(ch);
                    ch = Character.forDigit(ba[j] & 0xF, 16);
                    if (useUppercase && Character.isLetter(ch)) {
                        ch -= caseDiff;
                    }
                    out.append(ch);
                }
                charArrayWriter.reset();
                needToChange = true;
            }
        }

        return (needToChange ? out.toString() : input);
    }

}
