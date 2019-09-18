package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;

import java.util.*;
import java.util.function.Function;

/**
 * OCFL inventory object. It is intended to be used to encode and decode inventories. Inventories are immutable. Creating
 * a new version of an object requires creating a new inventory.
 *
 * @see InventoryBuilder
 * @see <a href="https://ocfl.io/">https://ocfl.io/</a>
 */
public class Inventory {

    private final String id;
    private final InventoryType type;
    private final DigestAlgorithm digestAlgorithm;
    private final VersionId head;
    private final String contentDirectory;

    // The digest map should be a TreeMap with case insensitive ordering
    private final Map<DigestAlgorithm, Map<String, Set<String>>> fixity;
    // This should be a TreeMap with case insensitive ordering
    private final Map<String, Set<String>> manifest;
    private final Map<VersionId, Version> versions;

    @JsonIgnore
    private final Map<String, String> reverseManifestMap;

    /**
     * This constructor is used by Jackson for deserialization.
     */
    @JsonCreator
    public Inventory(
            @JsonProperty("id") String id,
            @JsonProperty("type") InventoryType type,
            @JsonProperty("digestAlgorithm") DigestAlgorithm digestAlgorithm,
            @JsonProperty("head") VersionId head,
            @JsonProperty("contentDirectory") String contentDirectory,
            @JsonProperty("fixity") Map<DigestAlgorithm, Map<String, Set<String>>> fixity,
            @JsonProperty("manifest") Map<String, Set<String>> manifest,
            @JsonProperty("versions") Map<VersionId, Version> versions) {
        this(id, type, digestAlgorithm, head, contentDirectory, fixity, manifest, versions, null);
    }

    /**
     * @see InventoryBuilder
     */
    public Inventory(
            String id,
            InventoryType type,
            DigestAlgorithm digestAlgorithm,
            VersionId head,
            String contentDirectory,
            Map<DigestAlgorithm, Map<String, Set<String>>> fixity,
            Map<String, Set<String>> manifest,
            Map<VersionId, Version> versions,
            Map<String, String> reverseManifestMap) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        this.type = Enforce.notNull(type, "type cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.head = Enforce.notNull(head, "head cannot be null");
        this.contentDirectory = contentDirectory != null ? contentDirectory : OcflConstants.DEFAULT_CONTENT_DIRECTORY;
        this.fixity = copyFixity(fixity);
        this.manifest = Collections.unmodifiableMap(copyManifest(manifest, Collections::unmodifiableSet));
        this.versions = Collections.unmodifiableMap(copyVersions(versions));
        if (reverseManifestMap == null) {
            this.reverseManifestMap = createReverseManifestMap(this.manifest);
        } else {
            this.reverseManifestMap = Map.copyOf(reverseManifestMap);
        }
    }

    private Map<DigestAlgorithm, Map<String, Set<String>>> copyFixity(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
        var newFixity = new HashMap<DigestAlgorithm, Map<String, Set<String>>>();

        if (fixity != null) {
            fixity.forEach((k, v) -> {
                var treeMap = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
                treeMap.putAll(v);
                newFixity.put(k, Collections.unmodifiableMap(treeMap));
            });
        }

        return Collections.unmodifiableMap(newFixity);
    }

    private Map<String, Set<String>> copyManifest(Map<String, Set<String>> manifest, Function<Set<String>, Set<String>> pathSetCreator) {
        Enforce.notNull(manifest, "manifest cannot be null");
        var newManifest = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
        manifest.forEach((digest, paths) -> newManifest.put(digest, pathSetCreator.apply(new TreeSet<>(paths))));
        return newManifest;
    }

    private Map<VersionId, Version> copyVersions(Map<VersionId, Version> versions) {
        Enforce.notNull(versions, "versions cannot be null");
        var newVersions = new TreeMap<VersionId, Version>(Comparator.comparing(VersionId::toString));
        newVersions.putAll(versions);
        return newVersions;
    }

    private Map<String, String> createReverseManifestMap(Map<String, Set<String>> manifest) {
        var reverseMap = new HashMap<String, String>();
        manifest.forEach((digest, paths) -> paths.forEach(path -> reverseMap.put(path, digest)));
        return Collections.unmodifiableMap(reverseMap);
    }

    /**
     * The algorithm used to compute the digests that are used as file identifiers. sha512 be default.
     */
    @JsonGetter("digestAlgorithm")
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    /**
     * The object's id
     */
    @JsonGetter("id")
    public String getId() {
        return id;
    }

    /**
     * The version of the most recent version of the object. This is in the format of "vX" where "X" is a positive integer.
     */
    @JsonGetter("head")
    public VersionId getHead() {
        return head;
    }

    /**
     * The inventory's type and version.
     */
    @JsonGetter("type")
    public InventoryType getType() {
        return type;
    }

    /**
     * Contains the fixity information for all of the files that are part of the object.
     */
    @JsonGetter("fixity")
    public Map<DigestAlgorithm, Map<String, Set<String>>> getFixity() {
        return fixity;
    }

    @JsonIgnore
    public Map<DigestAlgorithm, Map<String, Set<String>>> getMutableFixity() {
        var newFixity = new HashMap<DigestAlgorithm, Map<String, Set<String>>>();

        if (fixity != null) {
            fixity.forEach((k, v) -> {
                var treeMap = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
                treeMap.putAll(v);
                newFixity.put(k, treeMap);
            });
        }

        return newFixity;
    }

    /**
     * A map of all of the files that are part of the object across all versions of the object. The map is keyed off file
     * digest and the value is the location of the file relative to the OCFL object root.
     */
    @JsonGetter("manifest")
    public Map<String, Set<String>> getManifest() {
        return manifest;
    }

    @JsonIgnore
    public Map<String, Set<String>> getMutableManifest() {
        return copyManifest(manifest, Function.identity());
    }

    @JsonIgnore
    public Map<String, String> getMutableReverseManifestMap() {
        return new HashMap<>(reverseManifestMap);
    }

    /**
     * A map of version identifiers to the object that describes the state of the object at that version. All versions of
     * the object are represented here.
     */
    @JsonGetter("versions")
    public Map<VersionId, Version> getVersions() {
        return versions;
    }

    @JsonIgnore
    public Map<VersionId, Version> getMutableVersions() {
        return new HashMap<>(versions);
    }

    /**
     * The name of the directory within a version directory that contains the object content. 'content' by default.
     */
    @JsonGetter("contentDirectory")
    public String getContentDirectory() {
        return contentDirectory;
    }

    @JsonIgnore
    public Version getHeadVersion() {
        return versions.get(head);
    }

    public Version getVersion(VersionId versionId) {
        return versions.get(versionId);
    }

    /**
     * Helper method for checking if an object contains a file with the given digest id.
     */
    public boolean manifestContainsId(String id) {
        return manifest.containsKey(id);
    }

    /**
     * Returns the digest that is used to identify the given path if it exists.
     */
    public String getFileId(String path) {
        return reverseManifestMap.get(path);
    }

    /**
     * Returns the set of paths that are identified by the given digest if they exist.
     */
    public Set<String> getFilePaths(String id) {
        return manifest.get(id);
    }

    /**
     * Returns the first path to a file that maps to the given digest
     */
    public String getFilePath(String id) {
        // There will only ever be one entry in this set unless de-dupping is turned off
        var paths = manifest.get(id);
        if (paths == null) {
            return null;
        }
        return paths.iterator().next();
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
