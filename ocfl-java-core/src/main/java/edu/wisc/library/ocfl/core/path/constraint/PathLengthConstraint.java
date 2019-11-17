package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.util.Enforce;

/**
 * Validates that a path or filename are not longer than a fixed number of characters or bytes
 */
public class PathLengthConstraint implements PathConstraint, FileNameConstraint {

    public enum Type {
        CHARACTERS("characters"),
        BYTES("bytes");

        private String value;

        Type(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private int maxLength;
    private Type type;

    /**
     * Creates a new constraint that limits the number of characters in a path
     *
     * @param maxLength maximum number of characters
     * @return constraint
     */
    public static PathLengthConstraint maxChars(int maxLength) {
        return new PathLengthConstraint(maxLength, Type.CHARACTERS);
    }

    /**
     * Creates a new constraint that limits the number of bytes in a path
     *
     * @param maxLength maximum number of bytes
     * @return constraint
     */
    public static PathLengthConstraint maxBytes(int maxLength) {
        return new PathLengthConstraint(maxLength, Type.BYTES);
    }

    public PathLengthConstraint(int maxLength, Type type) {
        this.maxLength = Enforce.expressionTrue(maxLength > 0, maxLength, "maxLength must be greater than 0");
        this.type = Enforce.notNull(type, "type cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String path) {
        if (type == Type.CHARACTERS) {
            validateLength(path.length(), message(path));
        } else {
            validateLength(path.getBytes().length, message(path));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (type == Type.CHARACTERS) {
            validateLength(fileName.length(), message(fileName, path));
        } else {
            validateLength(fileName.getBytes().length, message(fileName, path));
        }
    }

    private void validateLength(int length, String message) {
        if (length > maxLength) {
            throw new PathConstraintException(message);
        }
    }

    private String message(String path) {
        return String.format("The path is longer than %s %s: %s", maxLength, type, path);
    }

    private String message(String fileName, String path) {
        return String.format("The filename '%s' is longer than %s %s: %s", fileName, maxLength, type, path);
    }

}
