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

package edu.wisc.library.ocfl.core.extension.layout.config;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Objects;

/**
 * n-tuple storage layout encodes the object id and then splits it into n-character tuples that are used to construct a
 * nested path to the location the object is stored. This method is used to avoid performance problems associated with
 * having a large number of files in a single directory.
 *
 * @see DefaultLayoutConfig
 */
public class NTupleLayoutConfig implements LayoutConfig {

    private EncodingType encoding;
    private Casing casing;
    private DigestAlgorithm digestAlgorithm;
    private int size;
    private int depth;
    private EncapsulationConfig encapsulation;

    public NTupleLayoutConfig() {
        this.size = 0;
        this.depth = 0;
    }

    /**
     * The encoding to use on the object id
     *
     * @return encoding
     */
    @Override
    public EncodingType getEncoding() {
        return encoding;
    }

    /**
     * The encoding to use on the object id
     *
     * @param encoding encoding
     */
    public NTupleLayoutConfig setEncoding(EncodingType encoding) {
        this.encoding = encoding;
        return this;
    }

    /**
     * The casing to use on hex encoded strings
     *
     * @return casing
     */
    @Override
    public Casing getCasing() {
        return casing;
    }

    /**
     * The casing to use on hex encoded strings
     *
     * @param casing casing
     */
    public NTupleLayoutConfig setCasing(Casing casing) {
        this.casing = casing;
        return this;
    }

    /**
     * The digest algorithm ot use with {@link EncodingType#HASH}
     *
     * @return algorithm
     */
    @Override
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    /**
     * The digest algorithm ot use with {@link EncodingType#HASH}
     *
     * @param digestAlgorithm algorithm
     */
    public NTupleLayoutConfig setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    /**
     * The number of characters to use in a tuple.
     *
     * @return tuple size
     */
    public Integer getSize() {
        return size;
    }

    /**
     * The number of characters to use in a tuple.
     *
     * @param size tuple size
     */
    public NTupleLayoutConfig setSize(int size) {
        this.size = Enforce.expressionTrue(size >= 0, size, "size must be greater than or equal to 0");
        return this;
    }

    /**
     * The number of tuples before the object encapsulation directory. A depth of 0 means that entire encoded id will be
     * used to construct the storage path, it will not be truncated.
     *
     * @return depth
     */
    public Integer getDepth() {
        return depth;
    }

    /**
     * The number of tuples before the object encapsulation directory. A depth of 0 means that entire encoded id will be
     * used to construct the storage path, it will not be truncated.
     *
     * @param depth depth
     */
    public NTupleLayoutConfig setDepth(int depth) {
        this.depth = Enforce.expressionTrue(depth >= 0, depth, "depth must be greater than or equal to 0");
        return this;
    }

    /**
     * The encapsulation configuration
     *
     * @return configuration
     */
    public EncapsulationConfig getEncapsulation() {
        return encapsulation;
    }

    /**
     * The encapsulation configuration
     *
     * @param encapsulation configuration
     */
    public NTupleLayoutConfig setEncapsulation(EncapsulationConfig encapsulation) {
        this.encapsulation = encapsulation;
        return this;
    }

    @Override
    public String toString() {
        return "NTupleLayoutConfig{" +
                "encoding=" + encoding +
                ", casing=" + casing +
                ", digestAlgorithm=" + digestAlgorithm +
                ", size=" + size +
                ", depth=" + depth +
                ", encapsulation=" + encapsulation +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        NTupleLayoutConfig that = (NTupleLayoutConfig) o;
        return encoding == that.encoding &&
                casing == that.casing &&
                Objects.equals(digestAlgorithm, that.digestAlgorithm) &&
                Objects.equals(size, that.size) &&
                Objects.equals(depth, that.depth) &&
                Objects.equals(encapsulation, that.encapsulation);
    }

    @Override
    public int hashCode() {
        return Objects.hash(encoding, casing, digestAlgorithm, size, depth, encapsulation);
    }

}
