package edu.wisc.library.ocfl.api.util;

public final class Enforce {

    private Enforce() {

    }

    public static <T> T notNull(T object, String message) {
        if (object == null) {
            throw new NullPointerException(message);
        }
        return object;
    }

    public static String notBlank(String object, String message) {
        if (object == null || object.trim().isEmpty()) {
            throw new IllegalArgumentException(message);
        }
        return object;
    }

    public static <T> T expressionTrue(boolean expression, T object, String message) {
        if (!expression) {
            throw new IllegalArgumentException(message);
        }
        return object;
    }

}
