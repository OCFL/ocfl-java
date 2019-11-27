package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.encode.Encoder;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraints;

/**
 * Converts an object id into a path by simply encoding the id. This results in a large number of objects rooted in the
 * same directory, and should not be used outside of small repositories.
 */
public class FlatObjectIdPathMapper implements ObjectIdPathMapper {

    private Encoder encoder;
    private PathConstraintProcessor pathConstraints;

    public FlatObjectIdPathMapper(Encoder encoder) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
        this.pathConstraints = PathConstraints.noEmptyOrDotFilenames();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String map(String objectId) {
        return pathConstraints.apply(encoder.encode(objectId));
    }

}
