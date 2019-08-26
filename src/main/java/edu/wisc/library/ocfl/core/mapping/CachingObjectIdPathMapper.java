package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;

import java.nio.file.Path;

/**
 * Implementation of ObjectIdPathMapper that wraps a delegate ObjectIdPathMapper and caches all of the map requests.
 */
public class CachingObjectIdPathMapper implements ObjectIdPathMapper {

    private ObjectIdPathMapper delegate;
    private Cache<String, Path> cache;

    public CachingObjectIdPathMapper(ObjectIdPathMapper delegate, Cache<String, Path> cache) {
        this.delegate = Enforce.notNull(delegate, "delegate not null");
        this.cache = Enforce.notNull(cache, "cache cannot be null");
    }

    @Override
    public Path map(String objectId) {
        return cache.get(objectId, delegate::map);
    }

}
