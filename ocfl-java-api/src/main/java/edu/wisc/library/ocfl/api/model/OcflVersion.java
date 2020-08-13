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

import java.util.Arrays;

/**
 * Represents a version of the OCFL spec.
 */
public enum OcflVersion {

    OCFL_1_0("1.0", InventoryType.OCFL_1_0);

    private static final String OCFL_PREFIX = "ocfl_";
    private static final String OBJECT_PREFIX = "ocfl_object_";

    private final String versionString;
    private final InventoryType inventoryType;

    OcflVersion(String versionString, InventoryType inventoryType) {
        this.versionString = versionString;
        this.inventoryType = inventoryType;
    }

    /**
     * @return the OCFL version string as found in the Namaste file in the OCFL storage root
     */
    public String getOcflVersion() {
        return OCFL_PREFIX + versionString;
    }

    /**
     * @return the OCFL object version string as found in the Namaste file in the OCFL object root
     */
    public String getOcflObjectVersion() {
        return OBJECT_PREFIX + versionString;
    }

    /**
     * @return the InventoryType as specified in an object's inventory file
     */
    public InventoryType getInventoryType() {
        return inventoryType;
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
        for (var version : values()) {
            if (version.getOcflVersion().equals(ocflVersionString)) {
                return version;
            }
        }
        throw new IllegalArgumentException(String.format("Unable to map string '%s' to a known OCFL version. Known versions: %s",
                ocflVersionString, Arrays.asList(values())));
    }

}
