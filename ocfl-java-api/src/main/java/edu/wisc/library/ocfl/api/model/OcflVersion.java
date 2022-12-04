/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.exception.OcflJavaException;
import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * Represents a version of the OCFL spec.
 */
public enum OcflVersion {

    // The order versions are defined is significant as it affects the output of compareTo
    OCFL_1_0("1.0"),
    OCFL_1_1("1.1");

    private static final String OCFL_PREFIX = "ocfl_";
    private static final String OBJECT_PREFIX = "ocfl_object_";

    private final String versionString;
    private final String ocflVersion;
    private final String ocflObjectVersion;

    OcflVersion(String versionString) {
        this.versionString = versionString;
        this.ocflVersion = OCFL_PREFIX + versionString;
        this.ocflObjectVersion = OBJECT_PREFIX + versionString;
    }

    /**
     * @return the raw OCFL version number, eg 1.0
     */
    public String getRawVersion() {
        return versionString;
    }

    /**
     * @return the OCFL version string as found in the Namaste file in the OCFL storage root
     */
    public String getOcflVersion() {
        return ocflVersion;
    }

    /**
     * @return the OCFL object version string as found in the Namaste file in the OCFL object root
     */
    public String getOcflObjectVersion() {
        return ocflObjectVersion;
    }

    /**
     * @return the InventoryType as specified in an object's inventory file
     */
    public InventoryType getInventoryType() {
        switch (this) {
            case OCFL_1_0:
                return InventoryType.OCFL_1_0;
            case OCFL_1_1:
                return InventoryType.OCFL_1_1;
            default:
                throw new OcflJavaException("Unmapped version " + versionString);
        }
    }

    @Override
    public String toString() {
        return getOcflVersion();
    }

    /**
     * Returns an OCFL version based on the OCFL version string specified in the Namaste file in the OCFL storage root.
     *
     * @param ocflVersionString the version string from the Namaste file
     * @return OCFL version
     */
    public static OcflVersion fromOcflVersionString(String ocflVersionString) {
        var trimmed = ocflVersionString.trim();
        for (var version : values()) {
            if (version.getOcflVersion().equals(trimmed)) {
                return version;
            }
        }
        throw new OcflJavaException(String.format(
                "Unable to map string '%s' to a known OCFL version. Known versions: %s",
                ocflVersionString, Arrays.asList(values())));
    }

    /**
     * Returns an OCFL version based on the OCFL version string specified in the Namaste file in an OCFL object.
     *
     * @param ocflObjectVersionString the version string from the object Namaste file
     * @return OCFL version
     */
    public static OcflVersion fromOcflObjectVersionString(String ocflObjectVersionString) {
        var trimmed = ocflObjectVersionString.trim();
        for (var version : values()) {
            if (version.getOcflObjectVersion().equals(trimmed)) {
                return version;
            }
        }
        throw new OcflJavaException(String.format(
                "Unable to map string '%s' to a known OCFL object version. Known versions: %s",
                ocflObjectVersionString,
                Arrays.stream(values()).map(OcflVersion::getOcflObjectVersion).collect(Collectors.toList())));
    }

    /**
     * Returns an OCFL version based on the name of an OCFL storage root Namaste file
     *
     * @param ocflVersionFilename Namaste file name
     * @return OCFL version
     */
    public static OcflVersion fromOcflVersionFilename(String ocflVersionFilename) {
        var versionPart = ocflVersionFilename.substring(2);
        for (var version : values()) {
            if (version.getOcflVersion().equals(versionPart)) {
                return version;
            }
        }
        throw new OcflJavaException(String.format(
                "Unable to map string '%s' to a known OCFL version. Known versions: %s",
                versionPart, Arrays.asList(values())));
    }

    /**
     * Returns an OCFL version based on the name of an OCFL object Namaste file
     *
     * @param ocflObjectVersionFilename Namaste file name
     * @return OCFL version
     */
    public static OcflVersion fromOcflObjectVersionFilename(String ocflObjectVersionFilename) {
        var versionPart = ocflObjectVersionFilename.substring(2);
        for (var version : values()) {
            if (version.getOcflObjectVersion().equals(versionPart)) {
                return version;
            }
        }
        throw new OcflJavaException(String.format(
                "Unable to map string '%s' to a known OCFL object version. Known versions: %s",
                versionPart,
                Arrays.stream(values()).map(OcflVersion::getOcflObjectVersion).collect(Collectors.toList())));
    }
}
