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

package io.ocfl.api;

import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.SizeDigestAlgorithm;
import io.ocfl.api.model.StandardDigestAlgorithm;
import io.ocfl.api.util.Enforce;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry of digest algorithms. By default it contains all of the algorithms as defined in the OCFL spec and extensions.
 * Additional algorithms can be added as needed.
 */
public final class DigestAlgorithmRegistry {

    /*
     * From spec
     */
    public static final DigestAlgorithm md5 = new StandardDigestAlgorithm("md5", "md5");
    public static final DigestAlgorithm sha1 = new StandardDigestAlgorithm("sha1", "sha-1");
    public static final DigestAlgorithm sha256 = new StandardDigestAlgorithm("sha256", "sha-256");
    public static final DigestAlgorithm sha512 = new StandardDigestAlgorithm("sha512", "sha-512");
    public static final DigestAlgorithm blake2b512 = new StandardDigestAlgorithm("blake2b-512", "blake2b-512");

    /*
     * From extensions: https://ocfl.github.io/extensions/0009-digest-algorithms
     */
    public static final DigestAlgorithm blake2b160 = new StandardDigestAlgorithm("blake2b-160", "blake2b-160");
    public static final DigestAlgorithm blake2b256 = new StandardDigestAlgorithm("blake2b-256", "blake2b-256");
    public static final DigestAlgorithm blake2b384 = new StandardDigestAlgorithm("blake2b-384", "blake2b-384");
    public static final DigestAlgorithm sha512_256 = new StandardDigestAlgorithm("sha512/256", "sha-512/256");
    public static final DigestAlgorithm size = new SizeDigestAlgorithm();

    private static final Map<String, DigestAlgorithm> REGISTRY = new HashMap<>(Map.of(
            md5.getOcflName(), md5,
            sha1.getOcflName(), sha1,
            sha256.getOcflName(), sha256,
            sha512.getOcflName(), sha512,
            blake2b512.getOcflName(), blake2b512,
            blake2b160.getOcflName(), blake2b160,
            blake2b256.getOcflName(), blake2b256,
            blake2b384.getOcflName(), blake2b384,
            sha512_256.getOcflName(), sha512_256,
            size.getOcflName(), size));

    private DigestAlgorithmRegistry() {}

    /**
     * Adds a new algorithm to the registry
     *
     * @param algorithm algorithm to add to the registry
     */
    public static void register(DigestAlgorithm algorithm) {
        Enforce.notNull(algorithm, "algorithm cannot be null");
        REGISTRY.put(algorithm.getOcflName(), algorithm);
    }

    /**
     * Retrieves the algorithm that corresponds to the OCFL name from the registry
     *
     * @param ocflName name of the algorithm to retrieve
     * @return DigestAlgorithm
     */
    public static DigestAlgorithm getAlgorithm(String ocflName) {
        Enforce.notBlank(ocflName, "ocflName cannot be blank");
        return REGISTRY.get(ocflName.toLowerCase());
    }
}
