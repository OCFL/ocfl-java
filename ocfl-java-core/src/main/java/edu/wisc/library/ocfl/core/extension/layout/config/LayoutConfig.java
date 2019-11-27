package edu.wisc.library.ocfl.core.extension.layout.config;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

/**
 * Marker interface for layout extension configuration
 */
public interface LayoutConfig {

    /**
     * The type of encoding to use on object ids
     *
     * @return encoding
     */
    EncodingType getEncoding();

    /**
     * The case to use on hex strings
     *
     * @return casing
     */
    Casing getCasing();

    /**
     * The digest algorithm to use when digest based encoding is used
     *
     * @return algorithm
     */
    DigestAlgorithm getDigestAlgorithm();

}
