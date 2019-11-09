package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;

import java.util.Map;

/**
 * Implementation of ObjectIdPathMapper that wraps a delegate ObjectIdPathMapper and caches all of the map requests.
 */
public class CachingObjectIdPathMapper implements ObjectIdPathMapper {

    private ObjectIdPathMapper delegate;
    private Cache<String, String> cache;

    public CachingObjectIdPathMapper(ObjectIdPathMapper delegate, Cache<String, String> cache) {
        this.delegate = Enforce.notNull(delegate, "delegate not null");
        this.cache = Enforce.notNull(cache, "cache cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String map(String objectId) {
        return cache.get(objectId, delegate::map);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, Object> describeLayout() {
        return delegate.describeLayout();
    }
}
