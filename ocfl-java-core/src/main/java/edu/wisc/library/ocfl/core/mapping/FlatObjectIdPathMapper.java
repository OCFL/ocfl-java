package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Set;

/**
 * Converts an object id into a path by simply encoding the id. This results in a large number of objects rooted in the
 * same directory, and should not be used outside of small repositories.
 */
public class FlatObjectIdPathMapper implements ObjectIdPathMapper {

    private static final Set<String> INVALID_PATHS = Set.of(".", "..");

    private Encoder encoder;

    public FlatObjectIdPathMapper(Encoder encoder) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path map(String objectId) {
        return Paths.get(encodeObjectId(objectId));
    }

    @Override
    public Map<String, Object> describeLayout() {
        // TODO https://github.com/OCFL/spec/issues/351
        return Map.of(
                // TODO update to 'key' once the key is known
                "uri", "https://flat-layout",
                "description", "Flat Layout"
        );
    }

    private String encodeObjectId(String objectId) {
        var objectDir = encoder.encode(objectId);

        if (INVALID_PATHS.contains(objectDir)) {
            throw new IllegalArgumentException(String.format("Object id %s cannot be converted into a valid path.", objectId));
        }

        return objectDir;
    }

}
