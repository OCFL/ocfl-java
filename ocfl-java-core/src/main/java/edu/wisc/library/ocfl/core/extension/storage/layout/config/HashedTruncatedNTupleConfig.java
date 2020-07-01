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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleExtension;

import java.util.Objects;

/**
 * Configuration for the Hashed Truncated N-tuple Trees for OCFL Storage Hierarchies extension.
 *
 * TODO Add link to spec when finalized
 */
public class HashedTruncatedNTupleConfig implements OcflExtensionConfig {

    public enum CaseMapping {

        TO_UPPER("toUpper"),
        TO_LOWER("toLower");

        private final String value;

        CaseMapping(String value) {
            this.value = value;
        }

        @JsonCreator
        public static CaseMapping fromString(String value) {
            for (CaseMapping caseMapping : values()) {
                if (caseMapping.value.equals(value)) {
                    return caseMapping;
                }
            }

            throw new IllegalArgumentException("Unknown caseMapping: " + value);
        }

        @JsonValue
        public String getValue() {
            return value;
        }

        @Override
        public String toString() {
            return value;
        }

    }

    private DigestAlgorithm digestAlgorithm;
    private CaseMapping caseMapping;
    private int tupleSize;
    private int numberOfTuples;
    private boolean shortObjectRoot;

    /**
     * Creates a new config object with all of the default values set.
     */
    public HashedTruncatedNTupleConfig() {
        digestAlgorithm = DigestAlgorithm.sha256;
        caseMapping = CaseMapping.TO_LOWER;
        tupleSize = 3;
        numberOfTuples = 3;
        shortObjectRoot = false;
    }

    @JsonIgnore
    @Override
    public String getExtensionName() {
        return HashedTruncatedNTupleExtension.EXTENSION_NAME;
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
    public HashedTruncatedNTupleConfig setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        return this;
    }

    /**
     * @return the case mapping to use
     */
    public CaseMapping getCaseMapping() {
        return caseMapping;
    }

    /**
     * Indicates the casing to use for the hex encoded digest
     *
     * @param caseMapping the case mapping
     * @return this
     */
    public HashedTruncatedNTupleConfig setCaseMapping(CaseMapping caseMapping) {
        this.caseMapping = Enforce.notNull(caseMapping, "caseMapping cannot be null");
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
    public HashedTruncatedNTupleConfig setTupleSize(int tupleSize) {
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
    public HashedTruncatedNTupleConfig setNumberOfTuples(int numberOfTuples) {
        this.numberOfTuples = Enforce.expressionTrue(numberOfTuples >=0 && numberOfTuples <= 32,
                numberOfTuples, "numberOfTuples must be between 0 and 32 inclusive");
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
    public HashedTruncatedNTupleConfig setShortObjectRoot(boolean shortObjectRoot) {
        this.shortObjectRoot = shortObjectRoot;
        return this;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        HashedTruncatedNTupleConfig that = (HashedTruncatedNTupleConfig) o;
        return tupleSize == that.tupleSize &&
                numberOfTuples == that.numberOfTuples &&
                shortObjectRoot == that.shortObjectRoot &&
                digestAlgorithm.equals(that.digestAlgorithm) &&
                caseMapping == that.caseMapping;
    }

    @Override
    public int hashCode() {
        return Objects.hash(digestAlgorithm, caseMapping, tupleSize, numberOfTuples, shortObjectRoot);
    }

    @Override
    public String toString() {
        return "HashedTruncatedNTupleConfig{" +
                "digestAlgorithm=" + digestAlgorithm.getOcflName() +
                ", caseMapping=" + caseMapping +
                ", tupleSize=" + tupleSize +
                ", numberOfTuples=" + numberOfTuples +
                ", shortObjectRoot=" + shortObjectRoot +
                '}';
    }

}
