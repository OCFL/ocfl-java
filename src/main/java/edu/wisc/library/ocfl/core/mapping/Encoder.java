package edu.wisc.library.ocfl.core.mapping;

/**
 * Interface for encoding a string to make it safe to use as a directory name.
 *
 * @see UrlEncoder
 * @see PairTreeEncoder
 */
public interface Encoder {

    String encode(String input);

}
