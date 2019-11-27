package edu.wisc.library.ocfl.core.extension.layout.config;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

/**
 * Convenience class for layout configurations
 */
public final class DefaultLayoutConfig {

    public static final String DEFAULT_ENCAPSULATION_NAME = "obj";

    private DefaultLayoutConfig() {

    }

    /**
     * Flat layout with object ids URL encoded
     *
     * @return layout config
     */
    public static FlatLayoutConfig flatUrlConfig() {
        return new FlatLayoutConfig()
                .setEncoding(EncodingType.URL)
                .setCasing(Casing.LOWER);
    }

    /**
     * Flat layout with object ids pairtree encoded
     *
     * @return layout config
     */
    public static FlatLayoutConfig flatPairTreeConfig() {
        return new FlatLayoutConfig()
                .setEncoding(EncodingType.PAIRTREE)
                .setCasing(Casing.LOWER);
    }

    /**
     * Truncated n-tuple layout with object ids MD5 encoded. Each tuple is 2 characters, and the directories go 3 levels deep.
     * Objects are encapsulated in a URL encoded version of their object id.
     *
     * @return layout config
     */
    public static NTupleLayoutConfig nTupleHashConfig() {
        return new NTupleLayoutConfig()
                .setEncoding(EncodingType.HASH)
                .setCasing(Casing.LOWER)
                .setDigestAlgorithm(DigestAlgorithm.md5)
                .setSize(2)
                .setDepth(3)
                .setEncapsulation(new EncapsulationConfig()
                        .setDefaultString(DEFAULT_ENCAPSULATION_NAME)
                        .setType(EncapsulationType.ID)
                        .setEncoding(EncodingType.URL));
    }

    /**
     * Standard pairtree layout. Each tuple is 2 characters, and the directories are not truncated.
     * Objects are encapsulated in a 4 character substring of the pairtree cleaned object id.
     *
     * @return layout config
     */
    public static NTupleLayoutConfig pairTreeConfig() {
        return new NTupleLayoutConfig()
                .setEncoding(EncodingType.PAIRTREE)
                .setCasing(Casing.LOWER)
                .setSize(2)
                .setDepth(0)
                .setEncapsulation(new EncapsulationConfig()
                        .setDefaultString(DEFAULT_ENCAPSULATION_NAME)
                        .setType(EncapsulationType.SUBSTRING)
                        .setSubstringSize(4));
    }

}
