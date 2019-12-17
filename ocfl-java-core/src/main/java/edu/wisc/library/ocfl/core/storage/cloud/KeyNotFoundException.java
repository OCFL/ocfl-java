package edu.wisc.library.ocfl.core.storage.cloud;

/**
 * This exception is thrown when a cloud storage provider cannot find the specified key.
 */
public class KeyNotFoundException extends RuntimeException {

    public KeyNotFoundException() {
    }

    public KeyNotFoundException(String message) {
        super(message);
    }

    public KeyNotFoundException(String message, Throwable cause) {
        super(message, cause);
    }

    public KeyNotFoundException(Throwable cause) {
        super(cause);
    }

}
