package edu.wisc.library.ocfl.api.model;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.InvalidVersionException;
import org.junit.jupiter.api.Test;

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
        assertThrows(InvalidVersionException.class, () -> VersionNum.fromString("1"));
    }

    @Test
    public void shouldFailWhenHasExtraChars() {
        assertThrows(InvalidVersionException.class, () -> VersionNum.fromString("v1.2"));
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
        var versionNum = VersionNum.fromString("v00039");
        var nextVersion = versionNum.nextVersionNum();
        assertEquals("v00039", versionNum.toString());
        assertEquals("v00040", nextVersion.toString());
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
        assertThrows(InvalidVersionException.class, versionNum::previousVersionNum);
    }

    @Test
    public void shouldFailIncrementWhenNextVersionIsIllegalPaddedNumber() {
        var versionNum = VersionNum.fromString("v09999");
        assertThrows(InvalidVersionException.class, versionNum::nextVersionNum);
    }
}
