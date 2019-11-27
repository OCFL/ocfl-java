package edu.wisc.library.ocfl.core.extension.layout.config;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

import java.util.Objects;

/**
 * Layout configuration for a flat storage layout where every object is stored as a child of the root directory. It is
 * NOT RECOMMENDED to use this layout as it does not perform well when the repository contains lots of objects.
 */
public class FlatLayoutConfig implements LayoutConfig {

    private EncodingType encoding;
    private Casing casing;
    private DigestAlgorithm digestAlgorithm;


    /**
     * The encoding to use on the object id
     *
     * @return encoding
     */
    @Override
    public EncodingType getEncoding() {
        return encoding;
    }

    /**
     * The encoding to use on the object id
     *
     * @param encoding encoding
     */
    public FlatLayoutConfig setEncoding(EncodingType encoding) {
        this.encoding = encoding;
        return this;
    }

    /**
     * The casing to use on hex encoded strings
     *
     * @return casing
     */
    @Override
    public Casing getCasing() {
        return casing;
    }

    /**
     * The casing to use on hex encoded strings
     *
     * @param casing casing
     */
    public FlatLayoutConfig setCasing(Casing casing) {
        this.casing = casing;
        return this;
    }

    /**
     * The digest algorithm ot use with {@link EncodingType#HASH}
     *
     * @return algorithm
     */
    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    /**
     * The digest algorithm ot use with {@link EncodingType#HASH}
     *
     * @param digestAlgorithm algorithm
     */
    public FlatLayoutConfig setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    @Override
    public String toString() {
        return "FlatLayoutConfig{" +
                "encoding=" + encoding +
                ", casing=" + casing +
                ", digestAlgorithm=" + digestAlgorithm +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FlatLayoutConfig that = (FlatLayoutConfig) o;
        return encoding == that.encoding &&
                casing == that.casing &&
                Objects.equals(digestAlgorithm, that.digestAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, casing, digestAlgorithm);
    }

}
