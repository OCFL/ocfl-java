package edu.wisc.library.ocfl.core.mapping;

import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;

import java.nio.file.Path;
import java.time.Duration;

/**
 * Helper class for building ObjectIdPathMappers.
 */
public class ObjectIdPathMapperBuilder {

    private static final String DEFAULT_ENCAPSULATION_NAME = "obj";
    private static final int DEFAULT_ENCAPSULATION_LENGTH = 4;
    private static final int DEFAULT_HASH_DEPTH = 3;
    private static final int DEFAULT_HASH_SEGMENT_LENGTH = 3;

    private static final Duration DEFAULT_CACHE_DURATION = Duration.ofMinutes(10);
    private static final String DEFAULT_DIGEST_ALGORITHM = DigestAlgorithm.sha256.getJavaStandardName();

    private boolean useUppercase = false;
    private Cache<String, Path> cache;

    /**
     * Indicates whether or not characters in hex strings should be upper or lower case.
     *
     * <p>Default: false
     *
     * @param useUppercase whether or not hex characters should be in uppercase
     */
    public ObjectIdPathMapperBuilder useUppercase(boolean useUppercase) {
        this.useUppercase = useUppercase;
        return this;
    }

    /**
     * Configures the ObjectIdPathMapper to use a Caffeine cache with a duration of 10 minutes.
     */
    public ObjectIdPathMapperBuilder withDefaultCaffeineCache() {
        return withCaffeineCache(DEFAULT_CACHE_DURATION);
    }

    /**
     * Configures the ObjectIdPathMapper to use a Caffeine cache with a custom duration
     * @param expireAfterAccess
     */
    public ObjectIdPathMapperBuilder withCaffeineCache(Duration expireAfterAccess) {
        Enforce.notNull(expireAfterAccess, "expireAfterAccess cannot be null");
        cache = new CaffeineCache<>(Caffeine.newBuilder().expireAfterAccess(expireAfterAccess).build());
        return this;
    }

    /**
     * Configures the ObjectIdPathMapper to use a custom cache implementation
     * @param cache
     */
    public ObjectIdPathMapperBuilder withCustomCache(Cache<String, Path> cache) {
        this.cache = cache;
        return this;
    }

    /**
     * Builds a FlatObjectIdPathMapper using a UrlEncoder.
     */
    public ObjectIdPathMapper buildFlatMapper() {
        return applyCache(new FlatObjectIdPathMapper(new UrlEncoder(useUppercase)));
    }

    /**
     * Builds a PairTreeObjectIdPathMapper using a PairTreeEncoder, "obj" as the encapsulation string, and an encapsulation
     * length of 4.
     */
    public ObjectIdPathMapper buildDefaultPairTreeMapper() {
        return buildPairTreeMapper(DEFAULT_ENCAPSULATION_NAME, DEFAULT_ENCAPSULATION_LENGTH);
    }

    /**
     * Builds a PairTreeObjectIdPathMapper using a PairTreeEncoder and custom encapsulation configuration.
     *
     * @param encapsulationName The directory name to use to encapsulate an object when the encoded identifier is less than 3 characters long
     * @param encapsulationSubstringLength The number of characters from the end of an encoded identifier to use to encapsulate an object
     */
    public ObjectIdPathMapper buildPairTreeMapper(String encapsulationName, int encapsulationSubstringLength) {
        return applyCache(new PairTreeObjectIdPathMapper(
                new PairTreeEncoder(useUppercase), encapsulationName, encapsulationSubstringLength));
    }

    /**
     * Builds a HashingObjectIdPathMapper using sha256, 3 character segment length, and depth of 3.
     */
    public ObjectIdPathMapper buildDefaultTruncatedHashMapper() {
        return buildTruncatedHashMapper(DEFAULT_DIGEST_ALGORITHM, DEFAULT_HASH_DEPTH, DEFAULT_HASH_SEGMENT_LENGTH);
    }

    /**
     * Builds a HashingObjectIdPathMapper with a custom configuration.
     *
     * @param digestAlgorithm the digest algorithm to use on the object id
     * @param depth the number of directories deep that should be created
     * @param segmentLength the number of characters that should be in each directory name
     */
    public ObjectIdPathMapper buildTruncatedHashMapper(String digestAlgorithm, int depth, int segmentLength) {
        return applyCache(new HashingObjectIdPathMapper(digestAlgorithm, depth, segmentLength, useUppercase));
    }

    private ObjectIdPathMapper applyCache(ObjectIdPathMapper mapper) {
        if (cache != null) {
            return new CachingObjectIdPathMapper(mapper, cache);
        }
        return mapper;
    }

}
