package edu.wisc.library.ocfl.api.io;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.InputStream;
import java.security.DigestInputStream;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Wrapper around Java's DigestInputStream to compute a digest while streaming data, and then verify the fixity of the data.
 * After all of the stream's bytes have been read, {@code checkFixity()} should be called.
 */
public class FixityCheckInputStream extends DigestInputStream {

    private boolean enabled = true;
    private final String expectedDigestValue;

    /**
     * @param inputStream the underlying stream
     * @param digestAlgorithm the algorithm to use to calculate the digest (eg. sha512)
     * @param expectedDigestValue the expected digest value
     */
    public FixityCheckInputStream(InputStream inputStream, DigestAlgorithm digestAlgorithm, String expectedDigestValue) {
        super(inputStream, digestAlgorithm.getMessageDigest());
        this.expectedDigestValue = Enforce.notBlank(expectedDigestValue, "expectedDigestValue cannot be blank");
    }

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
     * <p>If the check is disabled, nothing happens
     *
     * @throws FixityCheckException when the actual digest value does not match the expected value
     */
    public void checkFixity() {
        if (enabled) {
            var actualDigest = Bytes.from(digest.digest()).encodeHex();
            if (!expectedDigestValue.equalsIgnoreCase(actualDigest)) {
                throw new FixityCheckException(String.format("Expected %s digest: %s; Actual: %s",
                        digest.getAlgorithm(), expectedDigestValue, actualDigest));
            }
        }

    }

    public String getExpectedDigestValue() {
        return expectedDigestValue;
    }

    /**
     * By default fixity checking is enabled. Use this method to disable it, and prevent needless digest computation
     *
     * @param enabled if fixity should be checked
     * @return this stream
     */
    public FixityCheckInputStream enableFixityCheck(boolean enabled) {
        on(enabled);
        return this;
    }

    @Override
    public void on(boolean on) {
        enabled = on;
        super.on(on);
    }

    @Override
    public String toString() {
        return "[Fixity Check Input Stream] expected: " + expectedDigestValue + "; actual: " + digest.toString();
    }

    private static MessageDigest messageDigest(String digestAlgorithm) {
        try {
            return MessageDigest.getInstance(digestAlgorithm);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

}
