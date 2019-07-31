package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.InventoryType;

public class OcflConstants {

    private OcflConstants() {

    }

    public static final String OCFL_VERSION = "ocfl_1.0";
    public static final String OCFL_OBJECT_VERSION = "ocfl_object_1.0";

    public static final String INVENTORY_FILE = "inventory.json";

    public static final String DEFAULT_CONTENT_DIRECTORY = "content";
    public static final InventoryType DEFAULT_INVENTORY_TYPE = InventoryType.OCFL_1_0;
    public static final DigestAlgorithm DEFAULT_DIGEST_ALGORITHM = DigestAlgorithm.sha512;

}
