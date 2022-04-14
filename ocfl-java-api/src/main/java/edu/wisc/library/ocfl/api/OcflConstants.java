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

package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.OcflVersion;

import java.util.Set;

public final class OcflConstants {

    private OcflConstants() {

    }

    public static final OcflVersion DEFAULT_OCFL_VERSION = OcflVersion.OCFL_1_1;

    public static final String OCFL_LAYOUT = "ocfl_layout.json";
    public static final String INVENTORY_FILE = "inventory.json";
    public static final String INVENTORY_SIDECAR_PREFIX = INVENTORY_FILE + ".";
    public static final String LOGS_DIR = "logs";
    public static final String EXTENSIONS_DIR = "extensions";
    public static final String EXT_CONFIG_JSON = "config.json";
    public static final String INIT_EXT = "init";

    public static final String OBJECT_NAMASTE_PREFIX = "0=ocfl_object_";

    public static final String DEFAULT_CONTENT_DIRECTORY = "content";
    public static final int DEFAULT_ZERO_PADDING_WIDTH = 0;

    public static final DigestAlgorithm DEFAULT_DIGEST_ALGORITHM = DigestAlgorithm.sha512;
    public static final Set<DigestAlgorithm> ALLOWED_DIGEST_ALGORITHMS = Set.of(DigestAlgorithm.sha512, DigestAlgorithm.sha256);

    public static final String MUTABLE_HEAD_EXT_NAME = "0005-mutable-head";
    public static final String MUTABLE_HEAD_EXT_PATH = EXTENSIONS_DIR + "/" + MUTABLE_HEAD_EXT_NAME;
    public static final String MUTABLE_HEAD_VERSION_PATH = MUTABLE_HEAD_EXT_PATH + "/head";
    public static final String MUTABLE_HEAD_REVISIONS_PATH = MUTABLE_HEAD_EXT_PATH + "/revisions";

    public static final String DIGEST_ALGORITHMS_EXT_NAME = "0001-digest-algorithms";

    public static final DigestAlgorithm[] VALID_INVENTORY_ALGORITHMS = new DigestAlgorithm[]{
            DigestAlgorithm.sha256, DigestAlgorithm.sha512
    };

}
