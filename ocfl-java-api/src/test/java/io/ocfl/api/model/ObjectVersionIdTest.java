package io.ocfl.api.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.ocfl.api.exception.InvalidVersionException;
import org.junit.jupiter.api.Test;

public class ObjectVersionIdTest {

    @Test
    public void shouldRejectBadVersions() {
        assertThrows(InvalidVersionException.class, () -> {
            ObjectVersionId.version("123", "10");
        });
    }

    @Test
    public void headVersionEquality() {
        assertEquals(ObjectVersionId.head("a"), ObjectVersionId.head("a"));
        assertNotEquals(ObjectVersionId.head("a"), ObjectVersionId.head("b"));
    }
}
