package edu.wisc.library.ocfl.core.path.constraint;

/**
 * Validates that a path meets a defined constraint
 */
public interface PathConstraint {

    /**
     * Validates that a path meets a defined constraint
     *
     * @param path the path to validate
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when the constraint is not met
     */
    void apply(String path);

}
