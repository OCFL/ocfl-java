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
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleIdExtension;

import java.util.Objects;

/**
 * Configuration for the Hashed Truncated N-tuple Trees with ID Encapsulation for OCFL Storage Hierarchies extension.
 *
 * TODO Add link to spec when finalized
 */
public class HashedTruncatedNTupleIdConfig implements OcflExtensionConfig {

    private DigestAlgorithm digestAlgorithm;
    private int tupleSize;
    private int numberOfTuples;

    /**
     * Creates a new config object with all of the default values set.
     */
    public HashedTruncatedNTupleIdConfig() {
        digestAlgorithm = DigestAlgorithm.sha256;
        tupleSize = 3;
        numberOfTuples = 3;
    }

    @JsonIgnore
    @Override
    public String getExtensionName() {
        return HashedTruncatedNTupleIdExtension.EXTENSION_NAME;
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
    public HashedTruncatedNTupleIdConfig setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
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
    public HashedTruncatedNTupleIdConfig setTupleSize(int tupleSize) {
        this.tupleSize = Enforce.expressionTrue(tupleSize >=0 && tupleSize <= 32,
                tupleSize, "tupleSize must be between 0 and 32 inclusive");
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
    public HashedTruncatedNTupleIdConfig setNumberOfTuples(int numberOfTuples) {
        this.numberOfTuples = Enforce.expressionTrue(numberOfTuples >=0 && numberOfTuples <= 32,
                numberOfTuples, "numberOfTuples must be between 0 and 32 inclusive");
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashedTruncatedNTupleIdConfig that = (HashedTruncatedNTupleIdConfig) o;
        return tupleSize == that.tupleSize &&
                numberOfTuples == that.numberOfTuples &&
                digestAlgorithm.equals(that.digestAlgorithm);
    }

    @Override
    public int hashCode() {
        return Objects.hash(digestAlgorithm, tupleSize, numberOfTuples);
    }

    @Override
    public String toString() {
        return "HashedTruncatedNTupleConfig{" +
                "digestAlgorithm=" + digestAlgorithm.getOcflName() +
                ", tupleSize=" + tupleSize +
                ", numberOfTuples=" + numberOfTuples +
                '}';
    }

}
