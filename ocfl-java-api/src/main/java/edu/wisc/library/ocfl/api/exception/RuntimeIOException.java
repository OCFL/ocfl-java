package edu.wisc.library.ocfl.api.exception;

public class RuntimeIOException extends RuntimeException {

    // TODO use this when wrapping IOException

    public RuntimeIOException() {
    }

    public RuntimeIOException(String message) {
        super(message);
    }

    public RuntimeIOException(String message, Throwable cause) {
        super(message, cause);
    }

    public RuntimeIOException(Throwable cause) {
        super(cause);
    }

}
