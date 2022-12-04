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

package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import java.util.HashMap;
import java.util.Map;

/**
 * Registry of digest algorithms. By default it contains all of the algorithms as defined in the OCFL spec and extensions.
 * Additional algorithms can be added as needed.
 */
public final class DigestAlgorithmRegistry {

    private static final Map<String, DigestAlgorithm> REGISTRY = new HashMap<>(Map.of(
            DigestAlgorithm.md5.getOcflName(), DigestAlgorithm.md5,
            DigestAlgorithm.sha1.getOcflName(), DigestAlgorithm.sha1,
            DigestAlgorithm.sha256.getOcflName(), DigestAlgorithm.sha256,
            DigestAlgorithm.sha512.getOcflName(), DigestAlgorithm.sha512,
            DigestAlgorithm.blake2b512.getOcflName(), DigestAlgorithm.blake2b512,
            DigestAlgorithm.blake2b160.getOcflName(), DigestAlgorithm.blake2b160,
            DigestAlgorithm.blake2b256.getOcflName(), DigestAlgorithm.blake2b256,
            DigestAlgorithm.blake2b384.getOcflName(), DigestAlgorithm.blake2b384,
            DigestAlgorithm.sha512_256.getOcflName(), DigestAlgorithm.sha512_256));

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
