package edu.wisc.library.ocfl.core.encode;

/**
 * Interface for encoding a string to make it safe to use as a directory name.
 *
 * @see UrlEncoder
 * @see PairTreeEncoder
 * @see DigestEncoder
 */
public interface Encoder {

    /**
     * Encodes the input string
     *
     * @param input string
     * @return encoded string
     */
    String encode(String input);

}
