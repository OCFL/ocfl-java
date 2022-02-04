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

package edu.wisc.library.ocfl.core.extension.storage.layout.config;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.NTupleOmitPrefixStorageLayoutExtension;

import java.util.Objects;

/**
 * Configuration for the <a href=
 * "https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.md">
 * N Tuple Storage Layout</a> extension.
 *
 * @author vcrema
 * @since 2021-10-25
 */
public class NTupleOmitPrefixStorageLayoutConfig implements OcflExtensionConfig {

    public enum ZeroPadding {
        LEFT, RIGHT;

        @JsonValue
        public String toJson() {
            return name().toLowerCase();
        }
    }

    private static final int DEFAULT_TUPLE_SIZE = 3;
    private static final int DEFAULT_NUM_TUPLES = 3;
    private static final ZeroPadding DEFAULT_PADDING = ZeroPadding.LEFT;
    private static final boolean DEFAULT_REVERSE_ROOT = false;

    private String delimiter;
    private int tupleSize;
    private int numberOfTuples;
    private ZeroPadding zeroPadding;
    private boolean reverseObjectRoot;

    public NTupleOmitPrefixStorageLayoutConfig() {
        this.tupleSize = DEFAULT_TUPLE_SIZE;
        this.numberOfTuples = DEFAULT_NUM_TUPLES;
        this.zeroPadding = DEFAULT_PADDING;
        this.reverseObjectRoot = DEFAULT_REVERSE_ROOT;
    }

    @Override
    public String getExtensionName() {
        return NTupleOmitPrefixStorageLayoutExtension.EXTENSION_NAME;
    }

    public void setExtensionName(String extensionName) {
        if (!Objects.equals(getExtensionName(), extensionName)) {
            throw new OcflExtensionException(String.format("The extension name for %s must be %s; found %s.",
                    getClass().getSimpleName(), getExtensionName(), extensionName));
        }
    }

    @JsonIgnore
    @Override
    public boolean hasParameters() {
        return true;
    }

    /**
     * @return the delimiter marking end of prefix
     */
    public String getDelimiter() {
        return delimiter;
    }

    /**
     * @return tupleSize - the segment size (in characters) to split the digest
     *         into
     */
    public int getTupleSize() {
        return tupleSize;
    }

    /**
     * @return The number of segments to use for path generation
     */
    public int getNumberOfTuples() {
        return numberOfTuples;
    }

    /**
     * return zeroPadding - Indicates whether to use left or right zero padding
     * for ids less than tupleSize * numberOfTuples
     */
    public ZeroPadding getZeroPadding() {
        return zeroPadding;
    }

    /**
     * return true or false, indicates that the prefix-omitted, padded object
     * identifier should be reversed
     */
    public boolean isReverseObjectRoot() {
        return reverseObjectRoot;
    }

    /**
     * The case-insensitive, delimiter marking the end of the OCFL object
     * identifier prefix; MUST consist of a character string of length one or
     * greater. If the delimiter is found multiple times in the OCFL object
     * identifier, its last occurrence (right-most) will be used to select the
     * termination of the prefix.
     *
     * @param delimiter
     *            marking the end of prefix
     */
    public NTupleOmitPrefixStorageLayoutConfig setDelimiter(String delimiter) {
        this.delimiter = Enforce.expressionTrue(delimiter != null && !delimiter.isEmpty(),
                delimiter, "delimiter must not be empty");

        return this;
    }

    /**
     * the segment size (in characters) to split the digest into
     *
     * @param tupleSize
     *            - the segment size (in characters) to split the digest into
     * 
     */
    public NTupleOmitPrefixStorageLayoutConfig setTupleSize(Integer tupleSize) {
        if (tupleSize == null) {
            this.tupleSize = DEFAULT_TUPLE_SIZE;
        } else {
            this.tupleSize = Enforce.expressionTrue(tupleSize >= 1 && tupleSize <= 32, tupleSize,
                    "tupleSize must be between 1 and 32 inclusive");
        }
        return this;
    }

    /**
     * The number of segments to use for path generation
     *
     * @param numberOfTuples
     *            - The number of segments to use for path generation
     * 
     */
    public NTupleOmitPrefixStorageLayoutConfig setNumberOfTuples(Integer numberOfTuples) {
        if (numberOfTuples == null) {
            this.numberOfTuples = DEFAULT_NUM_TUPLES;
        } else {
            this.numberOfTuples = Enforce.expressionTrue(numberOfTuples >= 1 && numberOfTuples <= 32, numberOfTuples,
                    "numberOfTuples must be between 1 and 32 inclusive");
        }
        return this;
    }

    /**
     * Indicates whether to use left or right zero padding for ids less than
     * tupleSize * numberOfTuples
     *
     * @param zeroPadding
     *
     */
    public NTupleOmitPrefixStorageLayoutConfig setZeroPadding(ZeroPadding zeroPadding) {
        if (zeroPadding == null) {
            this.zeroPadding = DEFAULT_PADDING;
        } else {
            this.zeroPadding = zeroPadding;
        }
        return this;
    }

    /**
     * indicates that the prefix-omitted, padded object identifier should be
     * reversed
     *
     * @param reverseObjectRoot
     *            - indicates that the prefix-omitted, padded object identifier
     *            should be reversed
     *
     */
    public NTupleOmitPrefixStorageLayoutConfig setReverseObjectRoot(Boolean reverseObjectRoot) {
        if (reverseObjectRoot == null) {
            this.reverseObjectRoot = DEFAULT_REVERSE_ROOT;
        } else {
            this.reverseObjectRoot = reverseObjectRoot;
        }
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        NTupleOmitPrefixStorageLayoutConfig that = (NTupleOmitPrefixStorageLayoutConfig) o;
        return delimiter.equals(that.delimiter)
                && tupleSize == that.tupleSize
                && numberOfTuples == that.numberOfTuples
                && zeroPadding == that.zeroPadding
                && reverseObjectRoot == that.reverseObjectRoot;
    }

    @Override
    public int hashCode() {
        return Objects.hash(delimiter, tupleSize, numberOfTuples, reverseObjectRoot, zeroPadding);
    }

    @Override
    public String toString() {
        return "NTupleOmitPrefixStorageLayoutConfig{ delimiter='" + delimiter
                + "', tupleSize='" + tupleSize
                + "', numberOfTuples='" + numberOfTuples
                + "', zeroPadding='" + zeroPadding
                + "', reverseObjectRoot='" + reverseObjectRoot + "' }";
    }

}