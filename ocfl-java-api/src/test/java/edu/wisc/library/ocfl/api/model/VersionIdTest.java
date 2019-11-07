package edu.wisc.library.ocfl.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionIdTest {

    @Test
    public void shouldCreateVersionWhenValidNoPadding() {
        var versionId = VersionId.fromString("v3");
        assertEquals("v3", versionId.toString());
    }

    @Test
    public void shouldCreateVersionWhenValidWithPadding() {
        var versionId = VersionId.fromString("v004");
        assertEquals("v004", versionId.toString());
    }

    @Test
    public void shouldFailWhenNoLeadingV() {
        assertThrows(IllegalArgumentException.class, () -> VersionId.fromString("1"));
    }

    @Test
    public void shouldFailWhenZero() {
        assertThrows(IllegalArgumentException.class, () -> VersionId.fromString("v0"));
    }

    @Test
    public void shouldFailWhenMultipleZeros() {
        assertThrows(IllegalArgumentException.class, () -> VersionId.fromString("v00"));
    }

    @Test
    public void shouldFailWhenHasExtraChars() {
        assertThrows(IllegalArgumentException.class, () -> VersionId.fromString("v1.2"));
    }

    @Test
    public void shouldIncrementVersionWhenNoPadding() {
        var versionId = VersionId.fromString("v3");
        var nextVersion = versionId.nextVersionId();
        assertEquals("v3", versionId.toString());
        assertEquals("v4", nextVersion.toString());
    }

    @Test
    public void shouldIncrementVersionWhenHasPadding() {
        var versionId = VersionId.fromString("v03");
        var nextVersion = versionId.nextVersionId();
        assertEquals("v03", versionId.toString());
        assertEquals("v04", nextVersion.toString());
    }

    @Test
    public void shouldDecrementVersionWhenNoPadding() {
        var versionId = VersionId.fromString("v3");
        var previousVersion = versionId.previousVersionId();
        assertEquals("v3", versionId.toString());
        assertEquals("v2", previousVersion.toString());
    }

    @Test
    public void shouldDecrementVersionWhenHasPadding() {
        var versionId = VersionId.fromString("v03");
        var previousVersion = versionId.previousVersionId();
        assertEquals("v03", versionId.toString());
        assertEquals("v02", previousVersion.toString());
    }

    @Test
    public void shouldFailDecrementWhenPreviousVersion0() {
        var versionId = VersionId.fromString("v1");
        assertThrows(IllegalStateException.class, versionId::previousVersionId);
    }

    @Test
    public void shouldFailIncrementWhenNextVersionIsIllegalPaddedNumber() {
        var versionId = VersionId.fromString("v09");
        assertThrows(IllegalStateException.class, versionId::nextVersionId);
    }

}
