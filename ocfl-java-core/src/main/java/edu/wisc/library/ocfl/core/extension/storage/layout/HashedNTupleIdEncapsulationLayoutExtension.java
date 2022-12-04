/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.extension.storage.layout;

import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.PercentEscaper;

/**
 * Implementation of the <a href="https://ocfl.github.io/extensions/0003-hash-and-id-n-tuple-storage-layout.html">
 * Hashed Truncated N-tuple Trees with Object ID Encapsulating Directory</a> extension.
 */
public class HashedNTupleIdEncapsulationLayoutExtension implements OcflStorageLayoutExtension {

    public static final String EXTENSION_NAME = "0003-hash-and-id-n-tuple-storage-layout";

    private static final int MAX_LENGTH = 100;
    private static final String SEPARATOR = "-";

    private final PercentEscaper percentEscaper;

    private HashedNTupleIdEncapsulationLayoutConfig config;

    public HashedNTupleIdEncapsulationLayoutExtension() {
        percentEscaper = PercentEscaper.builderWithSafeAlphaNumeric()
                .addSafeChars("-_")
                .plusForSpace(false)
                .useUppercase(false)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getExtensionName() {
        return EXTENSION_NAME;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getDescription() {
        return "OCFL object identifiers are hashed and encoded as lowercase hex strings."
                + " These digests are then divided into N n-tuple segments,"
                + " which are used to create nested paths under the OCFL storage root."
                + " Finally, the OCFL object identifier is percent-encoded to create a directory name for the OCFL object root.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(OcflExtensionConfig config) {
        if (this.config == null) {
            Enforce.notNull(config, "configFile cannot be null");

            if (!(config instanceof HashedNTupleIdEncapsulationLayoutConfig)) {
                throw new OcflExtensionException(String.format(
                        "This extension only supports %s configuration. Received: %s",
                        getExtensionConfigClass(), config));
            }

            var castConfig = (HashedNTupleIdEncapsulationLayoutConfig) config;

            validateConfig(castConfig);
            this.config = castConfig;
        }
    }

    @Override
    public Class<? extends OcflExtensionConfig> getExtensionConfigClass() {
        return HashedNTupleIdEncapsulationLayoutConfig.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapObjectId(String objectId) {
        if (config == null) {
            throw new OcflExtensionException("This extension must be initialized before it can be used.");
        }

        var digest = DigestUtil.computeDigestHex(config.getDigestAlgorithm(), objectId, false);

        var pathBuilder = new StringBuilder();

        for (var i = 0; i < config.getNumberOfTuples(); i++) {
            var start = i * config.getTupleSize();
            var end = start + config.getTupleSize();
            pathBuilder.append(digest, start, end).append("/");
        }

        pathBuilder.append(encapsulationDirectory(objectId, digest));

        return pathBuilder.toString();
    }

    private String encapsulationDirectory(String objectId, String digest) {
        var escapedId = percentEscaper.escape(objectId);

        if (escapedId.length() <= MAX_LENGTH) {
            return escapedId;
        }

        return escapedId.substring(0, MAX_LENGTH) + SEPARATOR + digest;
    }

    private static void validateConfig(HashedNTupleIdEncapsulationLayoutConfig config) {
        if (config != null) {
            if ((config.getTupleSize() == 0 || config.getNumberOfTuples() == 0)
                    && (config.getTupleSize() != 0 || config.getNumberOfTuples() != 0)) {
                throw new OcflExtensionException(String.format(
                        "If tupleSize (=%s) or numberOfTuples (=%s) is set to 0, then both must be 0.",
                        config.getTupleSize(), config.getNumberOfTuples()));
            }

            var totalTupleChars = config.getTupleSize() * config.getNumberOfTuples();
            var testDigest = DigestUtil.computeDigestHex(config.getDigestAlgorithm(), "test");

            if (totalTupleChars > testDigest.length()) {
                throw new OcflExtensionException(String.format(
                        "The config tupleSize=%s and numberOfTuples=%s requires"
                                + " a minimum of %s characters, but %s digests only have %s characters.",
                        config.getTupleSize(),
                        config.getNumberOfTuples(),
                        totalTupleChars,
                        config.getDigestAlgorithm().getOcflName(),
                        testDigest.length()));
            }
        }
    }
}
