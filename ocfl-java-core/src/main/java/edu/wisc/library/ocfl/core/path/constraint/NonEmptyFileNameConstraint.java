package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;

/**
 * Validates that there are no empty filenames
 */
public class NonEmptyFileNameConstraint implements FileNameConstraint {

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (fileName.isEmpty()) {
            throw new PathConstraintException(String.format("The path contains an illegal empty filename. Path: %s", path));
        }
    }

}
