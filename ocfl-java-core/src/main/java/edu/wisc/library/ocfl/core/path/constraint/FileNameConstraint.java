package edu.wisc.library.ocfl.core.path.constraint;

/**
 * Validates that a filename meets a defined constraint
 */
public interface FileNameConstraint {

    /**
     * Validates that a filename meets a defined constraint
     *
     * @param fileName the filename to validate
     * @param path the path the filename is part of. This is supplied for context and is not validated
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when the constraint is not met
     */
    void apply(String fileName, String path);

}
