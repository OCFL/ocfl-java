package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.DigestAlgorithmRegistry;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Maps OCFL defined digest algorithms to their Java names. Java does not include built-in implementations for all of the
 * algorithms, using a 3rd party provider, such as BouncyCastle, is necessary for some, such as blake2b.
 */
public class DigestAlgorithm {

    /*
     * From spec
     */
    public static final DigestAlgorithm md5 = new DigestAlgorithm("md5", "md5");
    public static final DigestAlgorithm sha1 = new DigestAlgorithm("sha1", "sha-1");
    public static final DigestAlgorithm sha256 = new DigestAlgorithm("sha256", "sha-256");
    public static final DigestAlgorithm sha512 = new DigestAlgorithm("sha512", "sha-512");
    public static final DigestAlgorithm blake2b512 = new DigestAlgorithm("blake2b-512", "blake2b-512");

    /*
     * From extensions: https://ocfl.github.io/extensions/0001-digest-algorithms
     */
    public static final DigestAlgorithm blake2b160 = new DigestAlgorithm("blake2b-160", "blake2b-160");
    public static final DigestAlgorithm blake2b256 = new DigestAlgorithm("blake2b-256", "blake2b-256");
    public static final DigestAlgorithm blake2b384 = new DigestAlgorithm("blake2b-384", "blake2b-384");
    public static final DigestAlgorithm sha512_256 = new DigestAlgorithm("sha512/256", "sha-512/256");

    private final String ocflName;
    private final String javaStandardName;

    public DigestAlgorithm(String ocflName) {
        this(ocflName, null);
    }

    public DigestAlgorithm(String ocflName, String javaStandardName) {
        this.ocflName = Enforce.notBlank(ocflName, "ocflName cannot be blank").toLowerCase();
        this.javaStandardName = javaStandardName;
    }

    @JsonCreator
    public static DigestAlgorithm fromOcflName(String ocflName) {
        var algorithm = DigestAlgorithmRegistry.getAlgorithm(ocflName);
        if (algorithm == null) {
            algorithm = new DigestAlgorithm(ocflName);
        }
        return algorithm;
    }

    /**
     * The OCFL name for the digest algorithm.
     */
    @JsonValue
    public String getOcflName() {
        return ocflName;
    }

    /**
     * The Java name for the digest algorithm.
     */
    public String getJavaStandardName() {
        return javaStandardName;
    }

    /**
     * Indicates whether or not there is a Java mapping for the algorithm.
     */
    public boolean hasJavaStandardName() {
        return javaStandardName != null;
    }

    /**
     * Returns a new MessageDigest
     */
    public MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(javaStandardName);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public String toString() {
        return "DigestAlgorithm{" +
                "ocflName='" + ocflName + '\'' +
                ", javaStandardName='" + javaStandardName + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DigestAlgorithm that = (DigestAlgorithm) o;
        return ocflName.equals(that.ocflName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ocflName);
    }

}
