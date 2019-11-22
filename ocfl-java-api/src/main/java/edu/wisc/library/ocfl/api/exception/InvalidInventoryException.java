package edu.wisc.library.ocfl.api.exception;

/**
 * This exception indicates that there is something wrong with an object's inventory.
 */
public class InvalidInventoryException extends RuntimeException {

    public InvalidInventoryException() {
    }

    public InvalidInventoryException(String message) {
        super(message);
    }

    public InvalidInventoryException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidInventoryException(Throwable cause) {
        super(cause);
    }

}
