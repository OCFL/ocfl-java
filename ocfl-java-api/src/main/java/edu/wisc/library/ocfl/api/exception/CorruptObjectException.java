package edu.wisc.library.ocfl.api.exception;

/**
 * This exception indicates that there is something wrong with the underlying OCFL object, and it cannot be operated on
 * by the ocfl-java library.
 */
public class CorruptObjectException extends RuntimeException {

    public CorruptObjectException() {
    }

    public CorruptObjectException(String message) {
        super(message);
    }

    public CorruptObjectException(String message, Throwable cause) {
        super(message, cause);
    }

    public CorruptObjectException(Throwable cause) {
        super(cause);
    }

}
