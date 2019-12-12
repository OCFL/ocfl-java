package edu.wisc.library.ocfl.core.encode;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.DigestUtil;

/**
 * Returns the hex encoded digest of the input string
 */
public class DigestEncoder implements Encoder {

    private DigestAlgorithm digestAlgorithm;
    private boolean useUppercase;

    /**
     * @param digestAlgorithm digest algorithm to use
     * @param useUppercase whether the hex string should be upper or lower case
     */
    public DigestEncoder(DigestAlgorithm digestAlgorithm, boolean useUppercase) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.useUppercase = useUppercase;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String encode(String input) {
        return DigestUtil.computeDigestHex(digestAlgorithm, input, useUppercase);
    }

}
