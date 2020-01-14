package edu.wisc.library.ocfl.test;

import org.junit.jupiter.api.function.Executable;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class OcflAsserts {

    public static <T extends Throwable> void assertThrowsWithMessage(Class<T> exception, String message, Executable executable) {
        assertThat(assertThrows(exception, executable).getMessage(), containsString(message));
    }

}
