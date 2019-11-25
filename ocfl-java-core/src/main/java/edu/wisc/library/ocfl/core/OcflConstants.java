package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

import java.util.Set;

public final class OcflConstants {

    private OcflConstants() {

    }

    public static final OcflVersion DEFAULT_OCFL_VERSION = OcflVersion.OCFL_1_0;

    public static final String INVENTORY_FILE = "inventory.json";

    public static final String DEFAULT_INITIAL_VERSION_ID = "v1";
    public static final String DEFAULT_CONTENT_DIRECTORY = "content";

    public static final DigestAlgorithm DEFAULT_DIGEST_ALGORITHM = DigestAlgorithm.sha512;
    public static final Set<DigestAlgorithm> ALLOWED_DIGEST_ALGORITHMS = Set.of(DigestAlgorithm.sha512, DigestAlgorithm.sha256);

    public static final String MUTABLE_HEAD_EXT_PATH = "extensions/mutable-head";
    public static final String MUTABLE_HEAD_VERSION_PATH = MUTABLE_HEAD_EXT_PATH + "/HEAD";

}
