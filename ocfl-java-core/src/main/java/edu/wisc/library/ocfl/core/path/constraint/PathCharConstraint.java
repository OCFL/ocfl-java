package edu.wisc.library.ocfl.core.path.constraint;

/**
 * Validates that the characters in a filename meet a defined constraint
 */
public interface PathCharConstraint {

    /**
     * Validates that the characters in a filename meet a defined constraint
     *
     * @param c the character to validate
     * @param path the path the character is part of. This is supplied for context and is not validated
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when the constraint is not met
     */
    void apply(char c, String path);

}
