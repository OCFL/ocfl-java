package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.*;

/**
 * Used to construct Inventory objects.
 */
public class InventoryBuilder {

    private String id;
    private InventoryType type;
    private DigestAlgorithm digestAlgorithm;
    private VersionId head;
    private String contentDirectory;

    private Map<DigestAlgorithm, Map<String, Set<String>>> fixity;
    private Map<String, Set<String>> manifest;
    private Map<VersionId, Version> versions;
    private Map<String, String> reverseManifestMap;

    public InventoryBuilder() {
        fixity = new HashMap<>();
        manifest = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        versions = new HashMap<>();
        reverseManifestMap = new HashMap<>();
    }

    /**
     * Used to construct a new Inventory that's based on an existing inventory. This should be used when creating a new
     * version of an existing object.
     *
     * @param original
     */
    public InventoryBuilder(Inventory original) {
        Enforce.notNull(original, "inventory cannot be null");
        this.id = original.getId();
        this.type = original.getType();
        this.digestAlgorithm = original.getDigestAlgorithm();
        this.head = original.getHead();
        this.contentDirectory = original.getContentDirectory();
        this.fixity = original.getMutableFixity();
        this.manifest = original.getMutableManifest();
        this.versions = original.getMutableVersions();
        this.reverseManifestMap = original.getMutableReverseManifestMap();
    }

    public void addNewHeadVersion(VersionId versionId, Version version) {
        Enforce.notNull(versionId, "versionId cannot be null");
        Enforce.notNull(version, "version cannot be null");

        versions.put(versionId, version);
        head = versionId;
    }

    public InventoryBuilder addFileToManifest(String id, String path) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(path, "path cannot be blank");

        manifest.computeIfAbsent(id, k -> new HashSet<>()).add(path);
        reverseManifestMap.put(path, id);
        return this;
    }

    public InventoryBuilder removeFileFromManifest(String path) {
        var digest = reverseManifestMap.remove(path);
        if (digest != null) {
            var paths = manifest.get(digest);
            if (paths.size() == 1) {
                manifest.remove(digest);
            } else {
                paths.remove(path);
            }
        }
        return this;
    }

    public InventoryBuilder addFixityForFile(String path, DigestAlgorithm algorithm, String value) {
        Enforce.notBlank(path, "path cannot be blank");
        Enforce.notNull(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be blank");

        fixity.computeIfAbsent(algorithm, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                .computeIfAbsent(value, k -> new HashSet<>()).add(path);
        return this;
    }

    public boolean manifestContainsId(String id) {
        return manifest.containsKey(id);
    }

    public InventoryBuilder id(String id) {
        this.id = id;
        return this;
    }

    public InventoryBuilder type(InventoryType type) {
        this.type = type;
        return this;
    }

    public InventoryBuilder digestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    public InventoryBuilder head(VersionId head) {
        this.head = head;
        return this;
    }

    public InventoryBuilder contentDirectory(String contentDirectory) {
        this.contentDirectory = contentDirectory;
        return this;
    }

    public InventoryBuilder fixity(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
        this.fixity = fixity;
        return this;
    }

    public InventoryBuilder manifest(Map<String, Set<String>> manifest) {
        this.manifest = manifest;
        return this;
    }

    public InventoryBuilder versions(Map<VersionId, Version> versions) {
        this.versions = versions;
        return this;
    }

    public String getId() {
        return id;
    }

    public VersionId getHead() {
        return head;
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public String getContentDirectory() {
        return contentDirectory;
    }

    public Inventory build() {
        return new Inventory(id, type, digestAlgorithm, head, contentDirectory, fixity, manifest, versions, reverseManifestMap);
    }

}
