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

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import edu.wisc.library.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;

/**
 * Implementation of the <a href="https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.md">
 * N Tuple Storage Layout</a> extension.
 *
 * @author vcrema
 */
public class NTupleOmitPrefixStorageLayoutExtension implements OcflStorageLayoutExtension {

    public static final String EXTENSION_NAME = "0007-n-tuple-omit-prefix-storage-layout";

    private static final Logger LOG = LoggerFactory.getLogger(NTupleOmitPrefixStorageLayoutExtension.class);
    
    private NTupleOmitPrefixStorageLayoutConfig config;

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
        return "This storage root extension describes an OCFL storage layout combining a pairtree-like root directory structure derived from prefix-omitted object identifiers, followed by the prefix-omitted object identifier themselves. The OCFL object identifiers are expected to contain prefixes which are removed in the mapping to directory names. The OCFL object identifier prefix is defined as all characters before and including a configurable delimiter. Where the prefix-omitted identifier length is less than tuple size * number of tuples, the remaining object id (prefix omitted) is left or right-side, zero-padded (configurable, left default), or not padded (none), and optionally reversed (default false). The object id is then divided into N n-tuple segments, and used to create nested paths under the OCFL storage root, followed by the prefix-omitted object id directory.";
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

            if (!(config instanceof NTupleOmitPrefixStorageLayoutConfig)) {
                throw new IllegalArgumentException(String.format("This extension only supports %s configuration. Received: %s",
                        getExtensionConfigClass(), config));
            }

            NTupleOmitPrefixStorageLayoutConfig castConfig = (NTupleOmitPrefixStorageLayoutConfig) config;

            validateConfig(castConfig);
            this.config = castConfig;
        }
    }

    private static void validateConfig(NTupleOmitPrefixStorageLayoutConfig config) {
        if (config != null) {
            if (StringUtils.isBlank(config.getDelimiter())) {
                throw new RuntimeException("Delimiter configuration must not be empty!");
            }
            if (config.getTupleSize() <= 0) {
                throw new RuntimeException("Character count configuration must not less than 0! Value given:" + config.getTupleSize());
            }
            if (config.getNumberOfTuples() <= 0) {
                throw new RuntimeException("Number of tuples configuration must not less than 0! Value given: " + config.getNumberOfTuples());
            }
            if (!config.getZeroPadding().equals(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_LEFT)
            		&& !config.getZeroPadding().equals(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_RIGHT)
            		&& !config.getZeroPadding().equals(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_NONE)) {
                throw new RuntimeException("Arg must not 'left', 'right', or 'none': 'zeroPadding'. Value given: " + config.getZeroPadding());
            }
        }
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
            throw new RuntimeException("This extension must be initialized before it can be used.");
        }
        if (!objectId.contains(config.getDelimiter())) {
        	throw new RuntimeException("The delimiter " + config.getDelimiter() + " cannot be found in " + objectId + ".");
        }
        //Split by delimiter and get the last part
        String[] parts = StringUtils. splitByWholeSeparator(objectId, config.getDelimiter());
        String section = parts[parts.length - 1];
        
        if (section.length() == 0) {
        	throw new RuntimeException("The delimiter " + config.getDelimiter() + " is only found at the end of " + objectId + ".");
        }
        
        if (config.reverseObjectRoot()) {
	        //Reverse the section
        	section = new StringBuilder(section).reverse().toString();
        }
        //Add padding if needed and requested
        if (section.length() < config.getTupleSize() * config.getNumberOfTuples()) {
        	
        	int paddingAmount = config.getTupleSize() * config.getNumberOfTuples();
        	if (config.getZeroPadding().equals(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_LEFT)) {
        		section = StringUtils.leftPad(section, paddingAmount, "0");
        	}
        	else if (config.getZeroPadding().equals(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_RIGHT)) {
        		section = StringUtils.rightPad(section, paddingAmount, "0");
        	}
        	//Throw runtime exception since we can't pad and there won't be enough characters for the pattern
        	else {
        		throw new RuntimeException("Zero padding is set to 'none' but " + section + " is too short to follow the requested tuple pattern: " + config.toString());
        	}
        }
        StringBuilder pathBuilder = new StringBuilder();
        //Split into even sections
        for (int i = 0; i < config.getNumberOfTuples(); i++) {
            int start = i * config.getTupleSize();
            int end = start + config.getTupleSize();
            pathBuilder.append(section, start, end).append("/");
        }

        //Append the original object id after the delimiter
        pathBuilder.append(parts[parts.length - 1]);
        return pathBuilder.toString();
    }

}