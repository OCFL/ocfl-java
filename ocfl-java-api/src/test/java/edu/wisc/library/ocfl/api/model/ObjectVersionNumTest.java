package edu.wisc.library.ocfl.api.model;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.InvalidVersionException;
import org.junit.jupiter.api.Test;

public class ObjectVersionNumTest {

    @Test
    public void shouldRejectBadVersions() {
        assertThrows(InvalidVersionException.class, () -> {
            ObjectVersionId.version("123", "10");
        });
    }
}
