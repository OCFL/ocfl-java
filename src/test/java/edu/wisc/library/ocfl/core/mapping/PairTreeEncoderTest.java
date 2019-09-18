package edu.wisc.library.ocfl.core.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PairTreeEncoderTest {

    Encoder encoder = new PairTreeEncoder(false);

    @Test
    public void shouldEncodeSpecialAsciiChars() {
        assertEquals("^22", encoder.encode("\""));
        assertEquals("^2a", encoder.encode("*"));
        assertEquals("^2b", encoder.encode("+"));
        assertEquals("^2c", encoder.encode(","));
        assertEquals("^3c", encoder.encode("<"));
        assertEquals("^3d", encoder.encode("="));
        assertEquals("^3e", encoder.encode(">"));
        assertEquals("^3f", encoder.encode("?"));
        assertEquals("^5c", encoder.encode("\\"));
        assertEquals("^5e", encoder.encode("^"));
        assertEquals("^7c", encoder.encode("|"));
        assertEquals("^20", encoder.encode(" "));
    }

    @Test
    public void shouldMapReservedChars() {
        assertEquals("=", encoder.encode("/"));
        assertEquals("+", encoder.encode(":"));
        assertEquals(",", encoder.encode("."));
    }

    @Test
    public void shouldEncodeUnicodeChars() {
        assertEquals("^c3^b5", encoder.encode("õ"));
        assertEquals("^c2^ac", encoder.encode("¬"));
    }

    @Test
    public void shouldNotEncodeVisibleAscii() {
        assertEquals("!", encoder.encode("!"));
        assertUnencodedRange('#', '*');
        assertEquals("-", encoder.encode("-"));
        assertUnencodedRange('0', ':');
        assertEquals(";", encoder.encode(";"));
        assertUnencodedRange('@', '\\');
        assertEquals("]", encoder.encode("]"));
        assertUnencodedRange('_', '|');
        assertEquals("}", encoder.encode("}"));
        assertEquals("~", encoder.encode("~"));
    }

    @Test
    public void shouldEncodeInUppercaseWhenConfigured() {
        encoder = new PairTreeEncoder(true);
        assertEquals("^C3^B5", encoder.encode("õ"));
        assertEquals("^C2^AC", encoder.encode("¬"));
    }

    private void assertUnencodedRange(char start, char end) {
        for (var c = start; c < end; c++) {
            assertEquals("" + c, encoder.encode("" + c));
        }
    }

}
