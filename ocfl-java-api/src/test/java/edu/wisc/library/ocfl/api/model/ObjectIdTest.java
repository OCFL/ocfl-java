package edu.wisc.library.ocfl.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class ObjectIdTest {

    @Test
    public void shouldRejectVersionsInTheWrongFormat() {
        assertThrows(IllegalArgumentException.class, () -> {
            ObjectId.version("123", "version 1");
        });
    }

    @Test
    public void shouldAllowVersionIdHead() {
        var objectId = ObjectId.version("123", "HEAD");
        assertTrue(objectId.isHead());
    }

}
