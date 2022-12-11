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
import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedNTupleLayoutExtension;
import java.util.Objects;

/**
 * Configuration for the <a href="https://ocfl.github.io/extensions/0004-hashed-n-tuple-storage-layout.html">
 * Hashed N-tuple Storage Layout</a> extension.
 */
public class HashedNTupleLayoutConfig implements OcflExtensionConfig {

    private static final DigestAlgorithm DEFAULT_DIGEST_ALGORITHM = DigestAlgorithm.sha256;
    private static final int DEFAULT_TUPLE_SIZE = 3;
    private static final int DEFAULT_NUM_TUPLES = 3;
    private static final boolean DEFAULT_SHORT_OBJ_ROOT = false;

    private DigestAlgorithm digestAlgorithm;
    private int tupleSize;
    private int numberOfTuples;
    private boolean shortObjectRoot;

    /**
     * Creates a new config object with all of the default values set.
     */
    public HashedNTupleLayoutConfig() {
        digestAlgorithm = DEFAULT_DIGEST_ALGORITHM;
        tupleSize = DEFAULT_TUPLE_SIZE;
        numberOfTuples = DEFAULT_NUM_TUPLES;
        shortObjectRoot = DEFAULT_SHORT_OBJ_ROOT;
    }

    @Override
    public String getExtensionName() {
        return HashedNTupleLayoutExtension.EXTENSION_NAME;
    }

    public void setExtensionName(String extensionName) {
        if (!Objects.equals(getExtensionName(), extensionName)) {
            throw new OcflExtensionException(String.format(
                    "The extension name for %s must be %s; found %s.",
                    getClass().getSimpleName(), getExtensionName(), extensionName));
        }
    }

    @JsonIgnore
    @Override
    public boolean hasParameters() {
        return true;
    }

    /**
     * @return the digest algorithm to use
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    /**
     * The digest algorithm to apply on the OCFL object identifier; MUST be an algorithm that is allowed in the OCFL fixity block
     *
     * @param digestAlgorithm the digest algorithm to use
     * @return this
     */
    public HashedNTupleLayoutConfig setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        if (digestAlgorithm == null) {
            this.digestAlgorithm = DEFAULT_DIGEST_ALGORITHM;
        } else {
            this.digestAlgorithm = digestAlgorithm;
        }
        return this;
    }

    /**
     * @return size of tuples in characters
     */
    public int getTupleSize() {
        return tupleSize;
    }

    /**
     * Indicates the size of the segments (in characters) that the digest is split into
     *
     * @param tupleSize size of tuples in characters
     * @return this
     */
    public HashedNTupleLayoutConfig setTupleSize(Integer tupleSize) {
        if (tupleSize == null) {
            this.tupleSize = DEFAULT_TUPLE_SIZE;
        } else {
            this.tupleSize = Enforce.expressionTrue(
                    tupleSize >= 0 && tupleSize <= 32, tupleSize, "tupleSize must be between 0 and 32 inclusive");
        }
        return this;
    }

    /**
     * @return number of tuples
     */
    public int getNumberOfTuples() {
        return numberOfTuples;
    }

    /**
     * Indicates how many segments are used for path generation
     *
     * @param numberOfTuples number of tuples
     * @return this
     */
    public HashedNTupleLayoutConfig setNumberOfTuples(Integer numberOfTuples) {
        if (numberOfTuples == null) {
            this.numberOfTuples = DEFAULT_NUM_TUPLES;
        } else {
            this.numberOfTuples = Enforce.expressionTrue(
                    numberOfTuples >= 0 && numberOfTuples <= 32,
                    numberOfTuples,
                    "numberOfTuples must be between 0 and 32 inclusive");
        }
        return this;
    }

    /**
     * @return true if a short object root should be used
     */
    public boolean isShortObjectRoot() {
        return shortObjectRoot;
    }

    /**
     * When true, indicates that the OCFL object root directory name should contain the remainder of the digest not used in the n-tuples segments
     *
     * @param shortObjectRoot whether or not to use a short object root
     * @return this
     */
    public HashedNTupleLayoutConfig setShortObjectRoot(Boolean shortObjectRoot) {
        this.shortObjectRoot = Objects.requireNonNullElse(shortObjectRoot, DEFAULT_SHORT_OBJ_ROOT);
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
        HashedNTupleLayoutConfig that = (HashedNTupleLayoutConfig) o;
        return tupleSize == that.tupleSize
                && numberOfTuples == that.numberOfTuples
                && shortObjectRoot == that.shortObjectRoot
                && digestAlgorithm.equals(that.digestAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(digestAlgorithm, tupleSize, numberOfTuples, shortObjectRoot);
    }

    @Override
    public String toString() {
        return "HashedNTupleLayoutConfig{" + "digestAlgorithm="
                + digestAlgorithm.getOcflName() + ", tupleSize="
                + tupleSize + ", numberOfTuples="
                + numberOfTuples + ", shortObjectRoot="
                + shortObjectRoot + '}';
    }
}
