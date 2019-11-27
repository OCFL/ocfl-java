package edu.wisc.library.ocfl.core.encode;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.charset.StandardCharsets;

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
        return Hex.encodeHexString(
                DigestUtils.digest(digestAlgorithm.getMessageDigest(), input.getBytes(StandardCharsets.UTF_8)),
                !useUppercase);
    }

}
