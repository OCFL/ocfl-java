package edu.wisc.library.ocfl.api.util;

import java.util.BitSet;

public final class Enforce {

    private static final BitSet WHITE_SPACE_CHARS = new BitSet(256) {{
        set(' ');
        set('\t');
        set('\n');
        set('\f');
        set('\r');
    }};

    private Enforce() {

    }

    public static <T> T notNull(T object, String message) {
        if (object == null) {
            throw new NullPointerException(message);
        }
        return object;
    }

    public static String notBlank(String object, String message) {
        if (object == null || isBlank(object)) {
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

    private static boolean isBlank(String value) {
        for (var c : value.toCharArray()) {
            if (!WHITE_SPACE_CHARS.get(c)) {
                return false;
            }
        }

        return true;
    }

}
