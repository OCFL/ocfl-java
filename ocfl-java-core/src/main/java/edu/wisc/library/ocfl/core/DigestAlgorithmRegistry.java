package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;

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
            DigestAlgorithm.sha512_256.getOcflName(), DigestAlgorithm.sha512_256
    ));

    private DigestAlgorithmRegistry() {

    }

    /**
     * Adds a new algorithm to the registry
     */
    public static void register(DigestAlgorithm algorithm) {
        Enforce.notNull(algorithm, "algorithm cannot be null");
        REGISTRY.put(algorithm.getOcflName(), algorithm);
    }

    /**
     * Retrieves the algorithm that corresponds to the OCFL name from the registry
     */
    public static DigestAlgorithm getAlgorithm(String ocflName) {
        Enforce.notBlank(ocflName, "ocflName cannot be blank");
        return REGISTRY.get(ocflName.toLowerCase());
    }

}
