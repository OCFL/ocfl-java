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

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import edu.wisc.library.ocfl.core.util.DigestUtil;

/**
 * Implementation of the Hashed Truncated N-tuple Trees for OCFL Storage Hierarchies extension.
 *
 * TODO add link to spec when finalized
 */
public class HashedTruncatedNTupleExtension implements OcflStorageLayoutExtension {

    public static final String EXTENSION_NAME = "0003-hashed-n-tuple-trees";

    private HashedTruncatedNTupleConfig config;

    /**
     * {@inheritDoc}
     */
    @Override
    public String getExtensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public String getDescription() {
        return "OCFL object identifiers are hashed and encoded as hex strings." +
                " These digests are then divided into N n-tuple segments," +
                " which are used to create nested paths under the OCFL storage root.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(OcflExtensionConfig config) {
        if (this.config == null) {
            Enforce.notNull(config, "configFile cannot be null");

            if (!(config instanceof HashedTruncatedNTupleConfig)) {
                throw new IllegalStateException(String.format("This extension only supports %s configuration. Received: %s",
                        getExtensionConfigClass(), config));
            }

            var castConfig = (HashedTruncatedNTupleConfig) config;

            validateConfig(castConfig);
            this.config = castConfig;
        }
    }

    @Override
    public Class<? extends OcflExtensionConfig> getExtensionConfigClass() {
        return HashedTruncatedNTupleConfig.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapObjectId(String objectId) {
        if (config == null) {
            throw new IllegalStateException("This extension must be initialized before it can be used.");
        }

        var digest = DigestUtil.computeDigestHex(config.getDigestAlgorithm(), objectId, useUpperCase());

        if (config.getTupleSize() == 0) {
            return digest;
        }

        return buildPath(digest);
    }

    private String buildPath(String digest) {
        var pathBuilder = new StringBuilder();

        for (var i = 0; i < config.getNumberOfTuples(); i++) {
            var start = i * config.getTupleSize();
            var end = start + config.getTupleSize();
            pathBuilder.append(digest, start, end).append("/");
        }

        if (config.isShortObjectRoot()) {
            var start = config.getNumberOfTuples() * config.getTupleSize();
            pathBuilder.append(digest, start, digest.length());
        } else {
            pathBuilder.append(digest);
        }

        return pathBuilder.toString();
    }

    private boolean useUpperCase() {
        return config.getCaseMapping() == HashedTruncatedNTupleConfig.CaseMapping.TO_UPPER;
    }

    private static void validateConfig(HashedTruncatedNTupleConfig config) {
        if (config != null) {
            if ((config.getTupleSize() == 0 || config.getNumberOfTuples() == 0)
                    && (config.getTupleSize() != 0 || config.getNumberOfTuples() != 0)) {
                throw new IllegalStateException(String.format("If tupleSize (=%s) or numberOfTuples (=%s) is set to 0, then both must be 0.",
                        config.getTupleSize(), config.getNumberOfTuples()));
            }

            var totalTupleChars = config.getTupleSize() * config.getNumberOfTuples();
            var testDigest = DigestUtil.computeDigestHex(config.getDigestAlgorithm(), "test");

            if (totalTupleChars > testDigest.length()) {
                throw new IllegalStateException(String.format("The config tupleSize=%s and numberOfTuples=%s requires" +
                        " a minimum of %s characters, but %s digests only have %s characters.",
                        config.getTupleSize(), config.getNumberOfTuples(),
                        totalTupleChars, config.getDigestAlgorithm().getOcflName(), testDigest.length()));
            }

            if (totalTupleChars == testDigest.length() && config.isShortObjectRoot()) {
                throw new IllegalStateException(String.format("The config tupleSize=%s and numberOfTuples=%s requires" +
                                " a minimum of %s characters, which is equal to the number of characters in a %s digest." +
                                " Therefore, shortObjectRoot cannot be set to true.",
                        config.getTupleSize(), config.getNumberOfTuples(),
                        totalTupleChars, config.getDigestAlgorithm().getOcflName()));
            }
        }
    }

}
