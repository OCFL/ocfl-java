/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.extension.storage.layout;

import io.ocfl.api.exception.OcflExtensionException;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.extension.OcflExtensionConfig;
import io.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Implementation of the <a href=
 * "https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.md">
 * N Tuple Storage Layout</a> extension.
 *
 * @author vcrema
 */
public class NTupleOmitPrefixStorageLayoutExtension implements OcflStorageLayoutExtension {

    private static final Logger LOG = LoggerFactory.getLogger(NTupleOmitPrefixStorageLayoutExtension.class);

    public static final String EXTENSION_NAME = "0007-n-tuple-omit-prefix-storage-layout";
    private static final Pattern ASCII_ONLY = Pattern.compile("\\A\\p{ASCII}*\\z");

    private NTupleOmitPrefixStorageLayoutConfig config;
    private String delimiter;
    private boolean caseMatters;

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
        return "This storage root extension describes an OCFL storage layout "
                + "combining a pairtree-like root directory structure derived from "
                + "prefix-omitted object identifiers, followed by the prefix-omitted "
                + "object identifier themselves. The OCFL object identifiers are "
                + "expected to contain prefixes which are removed in the mapping to "
                + "directory names. The OCFL object identifier prefix is defined as "
                + "all characters before and including a configurable delimiter. "
                + "Where the prefix-omitted identifier length is less than "
                + "tuple size * number of tuples, the remaining object id (prefix omitted) "
                + "is left or right-side, zero-padded (configurable, left default), "
                + "or not padded (none), and optionally reversed (default false). "
                + "The object id is then divided into N n-tuple segments, and used "
                + "to create nested paths under the OCFL storage root, followed by "
                + "the prefix-omitted object id directory.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(OcflExtensionConfig config) {
        Enforce.notNull(config, "configFile cannot be null");

        if (!(config instanceof NTupleOmitPrefixStorageLayoutConfig)) {
            throw new OcflExtensionException(String.format(
                    "This extension only supports %s configuration. Received: %s", getExtensionConfigClass(), config));
        }

        this.config = (NTupleOmitPrefixStorageLayoutConfig) config;
        this.delimiter = this.config.getDelimiter().toLowerCase();
        this.caseMatters = !this.delimiter.equals(this.config.getDelimiter());
    }

    @Override
    public Class<? extends OcflExtensionConfig> getExtensionConfigClass() {
        return NTupleOmitPrefixStorageLayoutConfig.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapObjectId(String objectId) {
        if (config == null) {
            throw new OcflExtensionException("This extension must be initialized before it can be used.");
        }
        if (!ASCII_ONLY.matcher(objectId).matches()) {
            throw new OcflExtensionException(String.format("This id %s must contain only ASCII characters.", objectId));
        }

        // Split by delimiter and get the last part
        String id = caseMatters ? objectId.toLowerCase() : objectId;
        int index = id.lastIndexOf(delimiter);

        if (index == -1) {
            throw new OcflExtensionException(
                    String.format("The delimiter %s cannot be found in %s.", delimiter, objectId));
        }

        String section = objectId.substring(index + delimiter.length());
        String baseObjectId = section;

        if (section.isEmpty()) {
            throw new OcflExtensionException(String.format(
                    "The object id <%s> is incompatible with layout extension %s because it produces an empty value.",
                    objectId, EXTENSION_NAME));
        }

        if (config.isReverseObjectRoot()) {
            // Reverse the section
            section = new StringBuilder(section).reverse().toString();
        }
        // Add padding if needed and requested
        if (section.length() < config.getTupleSize() * config.getNumberOfTuples()) {
            int paddingAmount = config.getTupleSize() * config.getNumberOfTuples();
            String padding = "0".repeat(paddingAmount - section.length());

            if (config.getZeroPadding() == NTupleOmitPrefixStorageLayoutConfig.ZeroPadding.RIGHT) {
                section = section + padding;
            } else {
                section = padding + section;
            }
        }
        StringBuilder pathBuilder = new StringBuilder();
        // Split into even sections
        for (int i = 0; i < config.getNumberOfTuples(); i++) {
            int start = i * config.getTupleSize();
            int end = start + config.getTupleSize();
            pathBuilder.append(section, start, end).append("/");
        }

        // Append the original object id after the delimiter
        pathBuilder.append(baseObjectId);
        return pathBuilder.toString();
    }
}
