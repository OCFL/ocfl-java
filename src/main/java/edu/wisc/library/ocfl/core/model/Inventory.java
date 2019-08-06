package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;

import java.util.*;

/**
 * OCFL inventory object. Conforms to the OCFL spec and is intended to be used to encode and decode inventories.
 * The OCFL spec has not been finalized yet and is subject to change.
 */
public class Inventory {

    private String id;
    private InventoryType type;
    private DigestAlgorithm digestAlgorithm;
    private VersionId head;
    private String contentDirectory;

    // The digest map should be a TreeMap with case insensitive ordering
    private Map<DigestAlgorithm, Map<String, Set<String>>> fixity;
    // This should be a TreeMap with case insensitive ordering
    private Map<String, Set<String>> manifest;
    private Map<VersionId, Version> versions;

    @JsonIgnore
    private Map<String, String> reverseManifestMap;

    public Inventory() {
        manifest = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        versions = new HashMap<>();
        fixity = new HashMap<>();
        reverseManifestMap = new HashMap<>();
    }

    public Inventory(String id) {
        this();
        this.id = Enforce.notBlank(id, "id cannot be blank");
    }

    /**
     * The algorithm used to compute the digests that are used as file identifiers. This is always sha512.
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public Inventory setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    /**
     * The object's id in the preservation system.
     */
    public String getId() {
        return id;
    }

    public Inventory setId(String id) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        return this;
    }

    /**
     * The version of the most recent version of the object. This is in the format of "vX" where "X" is a positive integer.
     */
    public VersionId getHead() {
        return head;
    }

    public Inventory setHead(VersionId head) {
        this.head = Enforce.notNull(head, "head cannot be blank");
        return this;
    }

    /**
     * The inventory's type. This is always "Object".
     */
    public InventoryType getType() {
        return type;
    }

    public Inventory setType(InventoryType type) {
        this.type = type;
        return this;
    }

    /**
     * Contains the fixity information for all of the files that are part of the object.
     */
    public Map<DigestAlgorithm, Map<String, Set<String>>> getFixity() {
        return fixity;
    }

    public Inventory setFixity(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
        Enforce.notNull(fixity, "fixity cannot be null");
        this.fixity = new HashMap<>();
        fixity.forEach((k, v) -> {
            var treeMap = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
            treeMap.putAll(v);
            this.fixity.put(k, treeMap);
        });
        return this;
    }

    /**
     * A map of all of the files that are part of the object across all versions of the object. The map is keyed off file
     * digest and the value is the location of the file. The value is a set, to conform to the OCFL spec, but will only
     * ever contain a single entry.
     */
    public Map<String, Set<String>> getManifest() {
        return manifest;
    }

    public Inventory setManifest(Map<String, Set<String>> manifest) {
        Enforce.notNull(manifest, "manifest cannot be null");
        this.manifest = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.manifest.putAll(manifest);
        this.manifest.forEach((digest, paths) -> paths.forEach(path -> reverseManifestMap.put(path, digest)));
        return this;
    }

    /**
     * A map of version identifiers to the object that describes the state of the object at that version. All versions of
     * the object are represented here.
     */
    public Map<VersionId, Version> getVersions() {
        return versions;
    }

    public Inventory setVersions(Map<VersionId, Version> versions) {
        this.versions = Enforce.notNull(versions, "versions cannot be null");
        return this;
    }

    public String getContentDirectory() {
        return contentDirectory != null ? contentDirectory : OcflConstants.DEFAULT_CONTENT_DIRECTORY;
    }

    public Inventory setContentDirectory(String contentDirectory) {
        this.contentDirectory = contentDirectory;
        return this;
    }

    public void addNewHeadVersion(VersionId versionId, Version version) {
        Enforce.notNull(versionId, "versionId cannot be null");
        Enforce.notNull(version, "version cannot be null");

        versions.put(versionId, version);
        head = versionId;
    }

    public Version headVersion() {
        return versions.get(head);
    }

    /**
     * Helper method for checking if an object contains a file with the given digest id.
     */
    public boolean manifestContainsId(String id) {
        return manifest.containsKey(id);
    }

    public Inventory addFileToManifest(String id, String path) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(path, "path cannot be blank");

        manifest.computeIfAbsent(id, k -> new HashSet<>()).add(path);
        reverseManifestMap.put(path, id);
        return this;
    }

    public Inventory addFixityForFile(String path, DigestAlgorithm algorithm, String value) {
        fixity.computeIfAbsent(algorithm, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
            .computeIfAbsent(value, k -> new HashSet<>()).add(path);
        return this;
    }

    public String getFileId(String path) {
        return reverseManifestMap.get(path);
    }

    @Override
    public String toString() {
        return "Inventory{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", digestAlgorithm='" + digestAlgorithm + '\'' +
                ", head='" + head + '\'' +
                ", contentDirectory='" + contentDirectory + '\'' +
                ", fixity=" + fixity +
                ", manifest=" + manifest +
                ", versions=" + versions +
                '}';
    }

}
