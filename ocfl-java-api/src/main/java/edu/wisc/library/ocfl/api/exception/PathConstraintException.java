package edu.wisc.library.ocfl.api.exception;

/**
 * Indicates when a path has failed a constraint check.
 */
public class PathConstraintException extends RuntimeException {

    public PathConstraintException() {
    }

    public PathConstraintException(String message) {
        super(message);
    }

    public PathConstraintException(String message, Throwable cause) {
        super(message, cause);
    }

    public PathConstraintException(Throwable cause) {
        super(cause);
    }

}
