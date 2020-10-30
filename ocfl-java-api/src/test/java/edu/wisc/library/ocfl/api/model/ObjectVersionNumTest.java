package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.exception.InvalidVersionException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ObjectVersionNumTest {

    @Test
    public void shouldRejectBadVersions() {
        assertThrows(InvalidVersionException.class, () -> {
            ObjectVersionId.version("123", "10");
        });
    }

}
