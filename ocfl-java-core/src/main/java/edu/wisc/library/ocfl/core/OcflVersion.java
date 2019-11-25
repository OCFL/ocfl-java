package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.core.model.InventoryType;

import java.util.Arrays;

/**
 * Represents a version of the OCFL spec.
 */
public enum OcflVersion {

    OCFL_1_0("1.0", InventoryType.OCFL_1_0);

    private static final String OCFL_PREFIX = "ocfl_";
    private static final String OBJECT_PREFIX = "ocfl_object_";

    private String versionString;
    private InventoryType inventoryType;

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
