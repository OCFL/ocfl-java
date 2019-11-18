package edu.wisc.library.ocfl.core.mapping;

import java.io.CharArrayWriter;
import java.nio.charset.StandardCharsets;
import java.util.BitSet;

/**
 * This was largely copied from URLEncoder and then modified to conform the the Pairtree spec.
 *
 * @see java.net.URLEncoder
 * @see <a href="https://tools.ietf.org/html/draft-kunze-pairtree-01">https://tools.ietf.org/html/draft-kunze-pairtree-01</a>
 */
public class PairTreeEncoder implements Encoder {

    private BitSet dontNeedEncoding;
    private final int caseDiff = ('a' - 'A');
    private boolean useUppercase;

    public PairTreeEncoder(boolean useUppercase) {
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

        dontNeedEncoding.set('!');
        dontNeedEncoding.set('#');
        dontNeedEncoding.set('$');
        dontNeedEncoding.set('%');
        dontNeedEncoding.set('&');
        dontNeedEncoding.set('\'');
        dontNeedEncoding.set('(');
        dontNeedEncoding.set(')');
        dontNeedEncoding.set('-');
        dontNeedEncoding.set(';');
        dontNeedEncoding.set('@');
        dontNeedEncoding.set('[');
        dontNeedEncoding.set(']');
        dontNeedEncoding.set('_');
        dontNeedEncoding.set('`');
        dontNeedEncoding.set('{');
        dontNeedEncoding.set('}');
        dontNeedEncoding.set('~');

        // special mappings
        dontNeedEncoding.set('/'); // => =
        dontNeedEncoding.set(':'); // => +
        dontNeedEncoding.set('.'); // => ,
    }

    /**
     * This was copied from URLEncoder.ecode() and slightly modified to conform the the Pairtree spec.
     *
     * https://tools.ietf.org/html/draft-kunze-pairtree-01
     *
     * @param input original string
     * @return encoded string
     */
    @Override
    public String encode(String input) {
        boolean needToChange = false;
        StringBuilder out = new StringBuilder(input.length());
        CharArrayWriter charArrayWriter = new CharArrayWriter();

        for (int i = 0; i < input.length();) {
            int c = input.charAt(i);
            if (dontNeedEncoding.get(c)) {
                // Special pairtree mappings
                switch (c) {
                    case '/':
                        c = '=';
                        needToChange = true;
                        break;
                    case ':':
                        c = '+';
                        needToChange = true;
                        break;
                    case '.':
                        c = ',';
                        needToChange = true;
                        break;
                    default:
                        break;
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
                    out.append('^');
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
