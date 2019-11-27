package edu.wisc.library.ocfl.core.encode;

/**
 * Returns the input string unchanged
 */
public class NoOpEncoder implements Encoder {

    /**
     * {@inheritDoc}
     */
    @Override
    public String encode(String input) {
        return input;
    }

}
