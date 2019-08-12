package edu.wisc.library.ocfl.api.exception;

public class ObjectOutOfSyncException extends RuntimeException {

    public ObjectOutOfSyncException() {
    }

    public ObjectOutOfSyncException(String message) {
        super(message);
    }

    public ObjectOutOfSyncException(String message, Throwable cause) {
        super(message, cause);
    }

    public ObjectOutOfSyncException(Throwable cause) {
        super(cause);
    }

}
