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

import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatOmitPrefixLayoutConfig;

/**
 * Implementation of the <a href="https://github.com/OCFL/extensions/blob/c9f2ef4fdb581d2a5070bc37af1ebe54150c998d/docs/NNNN-flat-omit-prefix-storage-layout.md">
 * Flat Omit Prefix Storage Layout</a> extension.
 *
 * @author awoods
 * @since 2021-06-22
 */
public class FlatOmitPrefixLayoutExtension implements OcflStorageLayoutExtension {

    public static final String EXTENSION_NAME = "NNNN-flat-omit-prefix-storage-layout";

    private FlatOmitPrefixLayoutConfig config;

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
        return "The OCFL object identifiers are expected to contain prefixes which are removed in the mapping to " +
                "directory names. The OCFL object identifier prefix is defined as all characters before and including a " +
                "configurable delimiter.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(OcflExtensionConfig config) {
        // Only set this.config if it is uninitialized
        if (this.config == null) {

            // Is arg config null?
            if (config == null) {
                throw new IllegalArgumentException("Arg config must not be null!");
            }

            if (!(config instanceof FlatOmitPrefixLayoutConfig)) {
                throw new IllegalArgumentException(String.format("This extension only supports %s configuration. Received: %s",
                        getExtensionConfigClass(), config));
            }

            FlatOmitPrefixLayoutConfig castConfig = (FlatOmitPrefixLayoutConfig) config;

            validateConfig(castConfig);
            this.config = castConfig;
        }
    }

    private static void validateConfig(FlatOmitPrefixLayoutConfig config) {
        if (config != null) {
            String delimiter = config.getDelimiter();
            if (delimiter == null || delimiter.isBlank()) {
                throw new RuntimeException("Digest configuration must not be empty!");
            }
        }
    }

    @Override
    public Class<? extends OcflExtensionConfig> getExtensionConfigClass() {
        return FlatOmitPrefixLayoutConfig.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapObjectId(String objectId) {
        if (config == null) {
            throw new RuntimeException("This extension must be initialized before it can be used.");
        }

        String[] parts = objectId.split(config.getDelimiter());
        return parts[parts.length - 1];
    }
}