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

package edu.wisc.library.ocfl.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.DigestAlgorithmRegistry;
import edu.wisc.library.ocfl.api.exception.OcflJavaException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Objects;

/**
 * Maps OCFL defined digest algorithms to their Java names. Java does not include built-in implementations for all of the
 * algorithms, using a 3rd party provider, such as BouncyCastle, is necessary for some, such as blake2b. New algorithms
 * should be registered in the {@link DigestAlgorithmRegistry}.
 */
public class DigestAlgorithm {

    /*
     * From spec
     */
    public static final DigestAlgorithm md5 = new DigestAlgorithm("md5", "md5");
    public static final DigestAlgorithm sha1 = new DigestAlgorithm("sha1", "sha-1");
    public static final DigestAlgorithm sha256 = new DigestAlgorithm("sha256", "sha-256");
    public static final DigestAlgorithm sha512 = new DigestAlgorithm("sha512", "sha-512");
    public static final DigestAlgorithm blake2b512 = new DigestAlgorithm("blake2b-512", "blake2b-512");

    /*
     * From extensions: https://ocfl.github.io/extensions/0001-digest-algorithms
     */
    public static final DigestAlgorithm blake2b160 = new DigestAlgorithm("blake2b-160", "blake2b-160");
    public static final DigestAlgorithm blake2b256 = new DigestAlgorithm("blake2b-256", "blake2b-256");
    public static final DigestAlgorithm blake2b384 = new DigestAlgorithm("blake2b-384", "blake2b-384");
    public static final DigestAlgorithm sha512_256 = new DigestAlgorithm("sha512/256", "sha-512/256");

    private final String ocflName;
    private final String javaStandardName;

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
            algorithm = new DigestAlgorithm(ocflName, null);
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
            algorithm = new DigestAlgorithm(ocflName, javaStandardName);
        }
        return algorithm;
    }

    private DigestAlgorithm(String ocflName, String javaStandardName) {
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

    /**
     * Returns a new MessageDigest
     *
     * @return MessageDigest
     */
    public MessageDigest getMessageDigest() {
        try {
            return MessageDigest.getInstance(javaStandardName);
        } catch (NoSuchAlgorithmException e) {
            throw new OcflJavaException("Failed to create message digest for: " + this, e);
        }
    }

    @Override
    public String toString() {
        return "DigestAlgorithm{" +
                "ocflName='" + ocflName + '\'' +
                ", javaStandardName='" + javaStandardName + '\'' +
                '}';
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
