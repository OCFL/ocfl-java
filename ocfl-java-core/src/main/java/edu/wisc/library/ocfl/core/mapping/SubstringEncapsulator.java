package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;

/**
 * Uses n characters from the end of the encoded id as the encapsulation directory
 */
public class SubstringEncapsulator implements Encapsulator {

    private int length;

    /**
     * @param length number of characters from the end of the encoded id to use
     */
    public SubstringEncapsulator(int length) {
        this.length = Enforce.expressionTrue(length > 1, length, "length must be greater than 1");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encapsulate(String objectId, String encodedId) {
        return encodedId.substring(Math.max(encodedId.length() - length, 0));
    }

}
