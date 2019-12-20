package edu.wisc.library.ocfl.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.regex.Pattern;

/**
 * Represents an OCFL version identifier. It is able to handle zero-padded version ids.
 */
public class VersionId implements Comparable<VersionId> {

    public static final VersionId V1 = new VersionId(1);

    private static final Pattern VALID_VERSION = Pattern.compile("^v\\d+$");

    private final long versionNumber;
    private final int zeroPaddingWidth;
    private final long maxVersion;
    private final String stringValue;

    /**
     * Creates a new VersionId from a version string in the format of "vN" where "N" is an integer greater than 0.
     * Zero-padded values are supported.
     *
     * @param value version string
     * @return version id
     */
    @JsonCreator
    public static VersionId fromString(String value) {
        if (!VALID_VERSION.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid VersionId: " + value);
        }

        var numPart = value.substring(1);
        var padding = 0;

        // Special case to allow v0 through
        if (!numPart.equals("0") && numPart.startsWith("0")) {
            padding = numPart.length();
        }

        return new VersionId(Long.valueOf(numPart), padding);
    }

    /**
     * Creates a new VersionId. Zero-padding is not used.
     *
     * @param versionNumber the version number, must be greater than 0
     */
    public VersionId(long versionNumber) {
        this(versionNumber, 0);
    }

    /**
     * Creates a new VersionId
     *
     * @param versionNumber the version number, must be greater than 0
     * @param zeroPaddingWidth the width of zero-padding, or 0 if the version is not zero-padded
     */
    public VersionId(long versionNumber, int zeroPaddingWidth) {
        this.versionNumber = Enforce.expressionTrue(versionNumber >= 0, versionNumber, "versionNumber must be greater than or equal to 0");
        this.zeroPaddingWidth = Enforce.expressionTrue(zeroPaddingWidth >= 0, zeroPaddingWidth, "zeroPaddingWidth must be greater than or equal to 0");

        if (zeroPaddingWidth == 0) {
            stringValue = "v" + versionNumber;
        } else {
            stringValue = String.format("v%0" + zeroPaddingWidth + "d", versionNumber);
        }

        if (zeroPaddingWidth == 0) {
            maxVersion = Long.MAX_VALUE;
        } else {
            maxVersion = (10 * (zeroPaddingWidth - 1)) - 1;
        }
    }

    /**
     * @return a new version id with an incremented version number
     */
    public VersionId nextVersionId() {
        var nextVersionNum = versionNumber + 1;
        if (nextVersionNum > maxVersion) {
            throw new IllegalStateException("Cannot increment version number. Current version " + toString() + " is the highest possible.");
        }
        return new VersionId(nextVersionNum, zeroPaddingWidth);
    }

    /**
     * @return a new version id with a decremented version number
     */
    public VersionId previousVersionId() {
        if (versionNumber == 1) {
            throw new IllegalStateException("Cannot decrement version number. Current version " + toString() + " is the lowest possible.");
        }
        return new VersionId(versionNumber - 1, zeroPaddingWidth);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VersionId versionId = (VersionId) o;
        return versionNumber == versionId.versionNumber;
    }

    @Override
    public int hashCode() {
        return Long.hashCode(versionNumber);
    }

    @JsonValue
    @Override
    public String toString() {
        return stringValue;
    }

    @Override
    public int compareTo(VersionId o) {
        return Long.compare(versionNumber, o.versionNumber);
    }

}
