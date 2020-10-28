package edu.wisc.library.ocfl.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ObjectVersionNumTest {

    @Test
    public void shouldRejectBadVersions() {
        assertThrows(IllegalArgumentException.class, () -> {
            ObjectVersionId.version("123", "10");
        });
    }

}
