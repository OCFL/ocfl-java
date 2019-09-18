package edu.wisc.library.ocfl.api.exception;

public class FixityCheckException extends RuntimeException {

    public FixityCheckException() {
    }

    public FixityCheckException(String message) {
        super(message);
    }

    public FixityCheckException(String message, Throwable cause) {
        super(message, cause);
    }

    public FixityCheckException(Throwable cause) {
        super(cause);
    }

}
