package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.InventoryType;

import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Contains OCFL related configuration options. All values are defaulted.
 */
public class OcflConfig {

    // TODO this should be changed in the version refactor
    private InventoryType defaultInventoryType;
    private DigestAlgorithm defaultDigestAlgorithm;
    private String defaultContentDirectory;
    private Set<DigestAlgorithm> fixityAlgorithms;

    public OcflConfig() {
        defaultInventoryType = OcflConstants.DEFAULT_INVENTORY_TYPE;
        defaultDigestAlgorithm = OcflConstants.DEFAULT_DIGEST_ALGORITHM;
        defaultContentDirectory = OcflConstants.DEFAULT_CONTENT_DIRECTORY;
        fixityAlgorithms = new HashSet<>();
    }

    /**
     * Set the default inventory type to use when creating new inventories.
     *
     * @param defaultInventoryType inventory type
     * @return config
     */
    public OcflConfig setDefaultInventoryType(InventoryType defaultInventoryType) {
        this.defaultInventoryType = Enforce.notNull(defaultInventoryType, "defaultInventoryType cannot be null");
        return this;
    }

    public InventoryType getDefaultInventoryType() {
        return defaultInventoryType;
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

    /**
     * Set the digest algorithms to use to commute additional fixity values for the fixity block. This should NOT be used
     * unless you need the fixity block, and the OCFL client does not use this block for fixity checking. Default: none
     *
     * @param fixityAlgorithms fixity algorithms
     * @return config
     */
    public OcflConfig setFixityAlgorithms(Set<DigestAlgorithm> fixityAlgorithms) {
        this.fixityAlgorithms = Enforce.notNull(fixityAlgorithms, "fixityAlgorithms cannot be null");
        return this;
    }

    public Set<DigestAlgorithm> getFixityAlgorithms() {
        return fixityAlgorithms;
    }

}
