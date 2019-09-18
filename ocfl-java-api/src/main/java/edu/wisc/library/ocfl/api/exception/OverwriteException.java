package edu.wisc.library.ocfl.api.exception;

public class OverwriteException extends RuntimeException {

    public OverwriteException() {
    }

    public OverwriteException(String message) {
        super(message);
    }

    public OverwriteException(String message, Throwable cause) {
        super(message, cause);
    }

    public OverwriteException(Throwable cause) {
        super(cause);
    }

}
