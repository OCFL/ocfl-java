package edu.wisc.library.ocfl.api.model;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class VersionNumTest {

    @Test
    public void shouldCreateVersionWhenValidNoPadding() {
        var versionNum = VersionNum.fromString("v3");
        assertEquals("v3", versionNum.toString());
    }

    @Test
    public void shouldCreateVersionWhenValidWithPadding() {
        var versionNum = VersionNum.fromString("v004");
        assertEquals("v004", versionNum.toString());
    }

    @Test
    public void shouldFailWhenNoLeadingV() {
        assertThrows(IllegalArgumentException.class, () -> VersionNum.fromString("1"));
    }

    @Test
    public void shouldFailWhenHasExtraChars() {
        assertThrows(IllegalArgumentException.class, () -> VersionNum.fromString("v1.2"));
    }

    @Test
    public void shouldIncrementVersionWhenNoPadding() {
        var versionNum = VersionNum.fromString("v3");
        var nextVersion = versionNum.nextVersionNum();
        assertEquals("v3", versionNum.toString());
        assertEquals("v4", nextVersion.toString());
    }

    @Test
    public void shouldIncrementVersionWhenHasPadding() {
        var versionNum = VersionNum.fromString("v03");
        var nextVersion = versionNum.nextVersionNum();
        assertEquals("v03", versionNum.toString());
        assertEquals("v04", nextVersion.toString());
    }

    @Test
    public void shouldDecrementVersionWhenNoPadding() {
        var versionNum = VersionNum.fromString("v3");
        var previousVersion = versionNum.previousVersionNum();
        assertEquals("v3", versionNum.toString());
        assertEquals("v2", previousVersion.toString());
    }

    @Test
    public void shouldDecrementVersionWhenHasPadding() {
        var versionNum = VersionNum.fromString("v03");
        var previousVersion = versionNum.previousVersionNum();
        assertEquals("v03", versionNum.toString());
        assertEquals("v02", previousVersion.toString());
    }

    @Test
    public void shouldFailDecrementWhenPreviousVersion0() {
        var versionNum = VersionNum.fromString("v1");
        assertThrows(IllegalStateException.class, versionNum::previousVersionNum);
    }

    @Test
    public void shouldFailIncrementWhenNextVersionIsIllegalPaddedNumber() {
        var versionNum = VersionNum.fromString("v09");
        assertThrows(IllegalStateException.class, versionNum::nextVersionNum);
    }

}
