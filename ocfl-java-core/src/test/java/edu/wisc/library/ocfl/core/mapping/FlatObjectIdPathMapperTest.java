package edu.wisc.library.ocfl.core.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class FlatObjectIdPathMapperTest {

    private FlatObjectIdPathMapper mapper;

    @BeforeEach
    public void setup() {
        mapper = new FlatObjectIdPathMapper(new UrlEncoder(true));
    }

    @Test
    public void shouldUrlEncodeIds() {
        var result = mapper.map("http://library.wisc.edu/#abc=1 2 3&cde=456");
        assertEquals("http%3A%2F%2Flibrary.wisc.edu%2F%23abc%3D1+2+3%26cde%3D456", result);
    }

    @Test
    public void shouldRejectDotId() {
        assertThrows(IllegalArgumentException.class, () -> mapper.map("."));
    }

    @Test
    public void shouldRejectDotDotId() {
        assertThrows(IllegalArgumentException.class, () -> mapper.map(".."));
    }

}
