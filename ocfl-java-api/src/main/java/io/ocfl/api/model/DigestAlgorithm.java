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

package io.ocfl.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import io.ocfl.api.DigestAlgorithmRegistry;
import io.ocfl.api.util.Enforce;
import java.security.MessageDigest;
import java.util.Objects;

/**
 * Maps OCFL defined digest algorithms to their Java names. Java does not include built-in implementations for all of the
 * algorithms, using a 3rd party provider, such as BouncyCastle, is necessary for some, such as blake2b. New algorithms
 * should be registered in the {@link DigestAlgorithmRegistry}.
 */
public abstract class DigestAlgorithm {

    private final String ocflName;
    private final String javaStandardName;

    /**
     * Creates a new {@link MessageDigest} that implements the digest algorithm
     *
     * @return new {@link MessageDigest}
     */
    public abstract MessageDigest getMessageDigest();

    /**
     * Encodes a binary digest value into a string representation.
     *
     * @param value digest value
     * @return the digest value as a string
     */
    public abstract String encode(byte[] value);

    /**
     * Creates a DigestAlgorithm for the given OCFL name. If the name is not mapped in the {@link DigestAlgorithmRegistry}
     * then a new object is created, but not automatically added to the registry. Newly created DigestAlgorithms are not
     * automatically mapped to Java names.
     *
     * @param ocflName ocfl name of algorithm
     * @return digest algorithm
     */
    @JsonCreator
    public static DigestAlgorithm fromOcflName(String ocflName) {
        var algorithm = DigestAlgorithmRegistry.getAlgorithm(ocflName);
        if (algorithm == null) {
            algorithm = new StandardDigestAlgorithm(ocflName, null);
        }
        return algorithm;
    }

    /**
     * Creates a DigestAlgorithm for the given OCFL name. If the name is not mapped in the {@link DigestAlgorithmRegistry}
     * then a new object is created, but not automatically added to the registry.
     *
     * @param ocflName ocfl name of algorithm
     * @param javaStandardName the name of the algorithm in Java
     * @return digest algorithm
     */
    public static DigestAlgorithm fromOcflName(String ocflName, String javaStandardName) {
        var algorithm = DigestAlgorithmRegistry.getAlgorithm(ocflName);
        if (algorithm == null) {
            algorithm = new StandardDigestAlgorithm(ocflName, javaStandardName);
        }
        return algorithm;
    }

    protected DigestAlgorithm(String ocflName, String javaStandardName) {
        this.ocflName = Enforce.notBlank(ocflName, "ocflName cannot be blank").toLowerCase();
        this.javaStandardName = javaStandardName;
    }

    /**
     * The OCFL name for the digest algorithm.
     *
     * @return standardized ocfl name for the digest algorithm
     */
    @JsonValue
    public String getOcflName() {
        return ocflName;
    }

    /**
     * The Java name for the digest algorithm.
     *
     * @return name of the digest algorithm in Java or null
     */
    public String getJavaStandardName() {
        return javaStandardName;
    }

    /**
     * Indicates whether or not there is a Java mapping for the algorithm.
     *
     * @return whether or not the digest algorithm has a Java name
     */
    public boolean hasJavaStandardName() {
        return javaStandardName != null;
    }

    @Override
    public String toString() {
        return "DigestAlgorithm{ocflName='" + ocflName + "', javaStandardName='" + getJavaStandardName() + "'}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        DigestAlgorithm that = (DigestAlgorithm) o;
        return ocflName.equals(that.ocflName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(ocflName);
    }
}
