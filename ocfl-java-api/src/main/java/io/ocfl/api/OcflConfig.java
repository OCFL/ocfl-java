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

package io.ocfl.api;

import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.OcflVersion;
import io.ocfl.api.util.Enforce;
import java.util.regex.Pattern;

/**
 * Contains OCFL related configuration options. All values are defaulted.
 */
public class OcflConfig {

    private OcflVersion ocflVersion;
    private DigestAlgorithm defaultDigestAlgorithm;
    private String defaultContentDirectory;
    private int defaultZeroPaddingWidth;
    private boolean upgradeObjectsOnWrite;

    public OcflConfig() {
        ocflVersion = null;
        defaultDigestAlgorithm = OcflConstants.DEFAULT_DIGEST_ALGORITHM;
        defaultContentDirectory = OcflConstants.DEFAULT_CONTENT_DIRECTORY;
        defaultZeroPaddingWidth = OcflConstants.DEFAULT_ZERO_PADDING_WIDTH;
        upgradeObjectsOnWrite = false;
    }

    public OcflConfig(OcflConfig original) {
        ocflVersion = original.ocflVersion;
        defaultDigestAlgorithm = original.defaultDigestAlgorithm;
        defaultContentDirectory = original.defaultContentDirectory;
        defaultZeroPaddingWidth = original.defaultZeroPaddingWidth;
        upgradeObjectsOnWrite = original.upgradeObjectsOnWrite;
    }

    /**
     * Set the default OCFL version to use when creating new inventories. If this value is null, then it's defaulted
     * to the OCFL version in the storage root.
     *
     * @param ocflVersion ocfl version
     * @return config
     */
    public OcflConfig setOcflVersion(OcflVersion ocflVersion) {
        this.ocflVersion = ocflVersion;
        return this;
    }

    public OcflVersion getOcflVersion() {
        return ocflVersion;
    }

    /**
     * Set the default digest algorithm to use when creating new inventories. MUST be sha-256 or sha-512. Default: sha-512
     *
     * @param defaultDigestAlgorithm digest algorithm
     * @return config
     */
    public OcflConfig setDefaultDigestAlgorithm(DigestAlgorithm defaultDigestAlgorithm) {
        Enforce.notNull(defaultDigestAlgorithm, "defaultDigestAlgorithm cannot be null");
        this.defaultDigestAlgorithm = Enforce.expressionTrue(
                OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(defaultDigestAlgorithm),
                defaultDigestAlgorithm,
                "Digest algorithm must be one of: " + OcflConstants.ALLOWED_DIGEST_ALGORITHMS);
        return this;
    }

    public DigestAlgorithm getDefaultDigestAlgorithm() {
        return defaultDigestAlgorithm;
    }

    /**
     * Set the default content directory to use when creating new inventories. MUST NOT contain / or \. Default: contents
     *
     * @param defaultContentDirectory content directory
     * @return config
     */
    public OcflConfig setDefaultContentDirectory(String defaultContentDirectory) {
        Enforce.notBlank(defaultContentDirectory, "contentDirectory cannot be blank");
        this.defaultContentDirectory = Enforce.expressionTrue(
                !Pattern.matches(".*[/\\\\].*", defaultContentDirectory),
                defaultContentDirectory,
                "Content directory cannot contain / or \\");
        return this;
    }

    public String getDefaultContentDirectory() {
        return defaultContentDirectory;
    }

    public int getDefaultZeroPaddingWidth() {
        return defaultZeroPaddingWidth;
    }

    /**
     * Set the default zero padding width to use in version numbers in newly created objects. Default: 0
     *
     * @param defaultZeroPaddingWidth zero padding width
     * @return config
     */
    public OcflConfig setDefaultZeroPaddingWidth(int defaultZeroPaddingWidth) {
        this.defaultZeroPaddingWidth = defaultZeroPaddingWidth;
        return this;
    }

    public boolean isUpgradeObjectsOnWrite() {
        return upgradeObjectsOnWrite;
    }

    /**
     * When set to true, existing objects that adhere to an older version of the OCFL spec will be upgraded to
     * the configured OCFL version when they are written to. For example, if the repository is configured for OCFL
     * 1.1, then existing 1.0 objects will be upgraded to 1.1 the next time they are written to. This is defaulted
     * to false.
     *
     * @param upgradeObjectsOnWrite true to upgrade existing OCFL objects on write
     */
    public OcflConfig setUpgradeObjectsOnWrite(boolean upgradeObjectsOnWrite) {
        this.upgradeObjectsOnWrite = upgradeObjectsOnWrite;
        return this;
    }

    @Override
    public String toString() {
        return "OcflConfig{" + "ocflVersion="
                + ocflVersion + ", defaultDigestAlgorithm="
                + defaultDigestAlgorithm + ", defaultContentDirectory='"
                + defaultContentDirectory + '\'' + ", defaultZeroPaddingWidth="
                + defaultZeroPaddingWidth + ", upgradeObjectsOnWrite="
                + upgradeObjectsOnWrite + '}';
    }
}
