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

import java.util.Objects;

import com.fasterxml.jackson.annotation.JsonIgnore;

import org.apache.commons.lang3.StringUtils;

import edu.wisc.library.ocfl.core.extension.storage.layout.NTupleOmitPrefixStorageLayoutExtension;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;

/**
 * Configuration for the <a href="https://ocfl.github.io/extensions/0007-n-tuple-omit-prefix-storage-layout.md">
 * N Tuple Storage Layout</a> extension.
 *
 * @author vcrema
 * @since 2021-10-25
 */
public class NTupleOmitPrefixStorageLayoutConfig implements OcflExtensionConfig {

	public static final String ZERO_PADDING_LEFT = "left";
	public static final String ZERO_PADDING_RIGHT = "right";
	public static final String ZERO_PADDING_NONE = "none";

	private String delimiter;
	private int tupleSize = 3;
	private int numberOfTuples = 3;
	private String zeroPadding = ZERO_PADDING_LEFT;
	private boolean reverseObjectRoot = false;

	@JsonIgnore
	@Override
	public String getExtensionName() {
		return NTupleOmitPrefixStorageLayoutExtension.EXTENSION_NAME;
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
	public String getZeroPadding() {
		return zeroPadding;
	}

	/**
	 * return true or false, indicates that the prefix-omitted, padded object
	 * identifier should be reversed
	 */
	public boolean reverseObjectRoot() {
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
		if (StringUtils.isBlank(delimiter)) {
			throw new IllegalArgumentException("Arg must not be empty or null: 'delimiter'");
		}
		this.delimiter = delimiter;
		return this;
	}

	/**
	 * the segment size (in characters) to split the digest into
	 *
	 * @param tupleSize
	 *            - the segment size (in characters) to split the digest into
	 * 
	 */
	public NTupleOmitPrefixStorageLayoutConfig setTupleSize(int tupleSize) {
		if (tupleSize <= 0) {
			throw new IllegalArgumentException("Arg must not be less than 1: 'tupleSize'. Given argument: " + tupleSize);
		}
		this.tupleSize = tupleSize;
		return this;
	}

	/**
	 * The number of segments to use for path generation
	 *
	 * @param numberOfTuples
	 *            - The number of segments to use for path generation
	 * 
	 */
	public NTupleOmitPrefixStorageLayoutConfig setNumberOfTuples(int numberOfTuples) {
		if (numberOfTuples <= 0) {
			throw new IllegalArgumentException("Arg must not be less than 1: 'numberOfTuples'. Given argument: " + numberOfTuples);
		}
		this.numberOfTuples = numberOfTuples;
		return this;
	}

	/**
	 * Indicates whether to use left or right zero padding for ids less than
	 * tupleSize * numberOfTuples
	 *
	 * @param zeroPadding
	 *
	 */
	public NTupleOmitPrefixStorageLayoutConfig setZeroPadding(String zeroPadding) {
		if (!zeroPadding.equals(ZERO_PADDING_LEFT) && !zeroPadding.equals(ZERO_PADDING_RIGHT) && !zeroPadding.equals(ZERO_PADDING_NONE)) {
			throw new IllegalArgumentException("Arg must not 'left', 'right', or 'none': 'zeroPadding'. Given argument:" + zeroPadding);
		}
		this.zeroPadding = zeroPadding;
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
	public NTupleOmitPrefixStorageLayoutConfig setReverseObjectRoot(boolean reverseObjectRoot) {
		this.reverseObjectRoot = reverseObjectRoot;
		return this;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o)
			return true;
		if (o == null || getClass() != o.getClass())
			return false;
		NTupleOmitPrefixStorageLayoutConfig that = (NTupleOmitPrefixStorageLayoutConfig) o;
		return delimiter.equals(that.delimiter) && tupleSize == that.tupleSize && numberOfTuples == that.numberOfTuples && zeroPadding == that.zeroPadding
				&& reverseObjectRoot == that.reverseObjectRoot;
	}

	@Override
	public int hashCode() {
		return Objects.hash(delimiter);
	}

	@Override
	public String toString() {
		return "NTupleOmitPrefixStorageLayoutConfig{ delimiter='" + delimiter + "', tupleSize='" + tupleSize + "', numberOfTuples='" + numberOfTuples
				+ "', zeroPadding='" + zeroPadding + "', reverseObjectRoot='" + reverseObjectRoot + "' }";
	}

}