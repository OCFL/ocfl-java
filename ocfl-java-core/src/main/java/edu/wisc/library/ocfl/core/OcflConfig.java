package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.regex.Pattern;

/**
 * Contains OCFL related configuration options. All values are defaulted.
 */
public class OcflConfig {

    private OcflVersion ocflVersion;
    private DigestAlgorithm defaultDigestAlgorithm;
    private String defaultContentDirectory;

    public OcflConfig() {
        ocflVersion = OcflConstants.DEFAULT_OCFL_VERSION;
        defaultDigestAlgorithm = OcflConstants.DEFAULT_DIGEST_ALGORITHM;
        defaultContentDirectory = OcflConstants.DEFAULT_CONTENT_DIRECTORY;
    }

    /**
     * Set the default OCFL version to use when creating new inventories.
     *
     * @param ocflVersion ocfl version
     * @return config
     */
    public OcflConfig setOcflVersion(OcflVersion ocflVersion) {
        this.ocflVersion = Enforce.notNull(ocflVersion, "ocflVersion cannot be null");
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
                OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(defaultDigestAlgorithm), defaultDigestAlgorithm,
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
        this.defaultContentDirectory = Enforce.expressionTrue(!Pattern.matches(".*[/\\\\].*", defaultContentDirectory), defaultContentDirectory,
                "Content directory cannot contain / or \\");
        return this;
    }

    public String getDefaultContentDirectory() {
        return defaultContentDirectory;
    }

}
