package edu.wisc.library.ocfl.core.encode;

import com.google.common.escape.Escaper;

/**
 * Encodes a string using the pairtree cleaning algorithm
 *
 * @see <a href="https://tools.ietf.org/html/draft-kunze-pairtree-01">https://tools.ietf.org/html/draft-kunze-pairtree-01</a>
 */
public class PairTreeEncoder implements Encoder {

    private static final String SAFE_CHARS = "!#$%&'()-;@[]_`{}~";

    private Escaper escaper;

    /**
     * @param useUppercase use upper or lower case in escape sequence
     */
    public PairTreeEncoder(boolean useUppercase) {
        this.escaper = PercentEscaper.builder()
                .encodeStartChar('^')
                .safeChars(SAFE_CHARS)
                .plusForSpace(false)
                .useUppercase(useUppercase)
                .addEscape('/', "=")
                .addEscape(':', "+")
                .addEscape('.', ",")
                .build();
    }

    /**
     * https://tools.ietf.org/html/draft-kunze-pairtree-01
     *
     * @param input original string
     * @return encoded string
     */
    @Override
    public String encode(String input) {
        return escaper.escape(input);
    }

}
