package edu.wisc.library.ocfl.api.io;

import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.util.Enforce;
import org.apache.commons.codec.binary.Hex;

import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Wrapper around Java's DigestInputStream to compute a digest while streaming data, and then verify the fixity of the data.
 * After all of the stream's bytes have been read, {@code checkFixity()} should be called.
 */
public class FixityCheckInputStream extends DigestInputStream {

    private final String expectedDigestValue;

    /**
     * @param inputStream the underlying stream
     * @param digestAlgorithm the algorithm to use to calculate the digest (eg. sha512)
     * @param expectedDigestValue the expected digest value
     */
    public FixityCheckInputStream(InputStream inputStream, String digestAlgorithm, String expectedDigestValue) {
        super(inputStream, messageDigest(digestAlgorithm));
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

    /**
     * Performs a fixity check and throws an exception if the check fails. This should only be called after the entire
     * contents of the stream has been read.
     *
     * @throws FixityCheckException when the actual digest value does not match the expected value
     */
    public void checkFixity() {
        var actualDigest = Hex.encodeHexString(digest.digest());
        if (!expectedDigestValue.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Expected %s digest: %s; Actual: %s",
                    digest.getAlgorithm(), expectedDigestValue, actualDigest));
        }
    }

    public String getExpectedDigestValue() {
        return expectedDigestValue;
    }

    private static MessageDigest messageDigest(String digestAlgorithm) {
        try {
            return MessageDigest.getInstance(digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "[Fixity Check Input Stream] expected: " + expectedDigestValue + "; actual: " + digest.toString();
    }
}
