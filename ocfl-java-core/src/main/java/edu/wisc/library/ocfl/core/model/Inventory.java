package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.nio.file.Path;
import java.util.*;
import java.util.function.Function;

/**
 * OCFL inventory object. It is intended to be used to encode and decode inventories. Inventories are immutable. Creating
 * a new version of an object requires creating a new inventory.
 *
 * @see InventoryBuilder
 * @see <a href="https://ocfl.io/">https://ocfl.io/</a>
 */
@JsonDeserialize(builder = Inventory.JacksonBuilder.class)
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
    private final RevisionId revisionId;

    @JsonIgnore
    private final boolean mutableHead;

    // TODO Should this be computed on demand?
    @JsonIgnore
    private final Map<String, String> reverseManifestMap;

    @JsonIgnore
    private final String objectRootPath;

    public static InventoryBuilder builder() {
        return new InventoryBuilder();
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
            boolean mutableHead,
            RevisionId revisionId,
            String objectRootPath,
            Map<String, String> reverseManifestMap) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        this.type = Enforce.notNull(type, "type cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        Enforce.expressionTrue(OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(digestAlgorithm), digestAlgorithm,
                "digestAlgorithm must be sha512 or sha256");
        this.head = Enforce.notNull(head, "head cannot be null");
        this.contentDirectory = contentDirectory;
        this.fixity = copyFixity(fixity);
        this.manifest = Collections.unmodifiableMap(copyManifest(manifest, Collections::unmodifiableSet));
        this.versions = Collections.unmodifiableMap(copyVersions(versions));

        this.mutableHead = mutableHead;
        this.revisionId = revisionId;
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be null");

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
     * Use {@code resolveContentDirectory()} instead
     */
    @JsonGetter("contentDirectory")
    public String getContentDirectory() {
        return contentDirectory;
    }

    /**
     * The name of the directory within a version directory that contains the object content. 'content' by default.
     */
    public String resolveContentDirectory() {
        if (contentDirectory == null) {
            return OcflConstants.DEFAULT_CONTENT_DIRECTORY;
        }
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
     * Returns the digest that is used to identify the given path if it exists.
     */
    public String getFileId(Path path) {
        return reverseManifestMap.get(FileUtil.pathToStringStandardSeparator(path));
    }

    /**
     * Returns the set of paths that are identified by the given digest if they exist.
     */
    public Set<String> getContentPaths(String id) {
        return manifest.get(id);
    }

    /**
     * Returns the first path to a file that maps to the given digest
     */
    public String getContentPath(String id) {
        // There will only ever be one entry in this set unless dedupping is turned off
        var paths = manifest.get(id);
        if (paths == null) {
            return null;
        }
        return paths.iterator().next();
    }

    /**
     * Returns the set of file ids of files that have content paths that begin with the given prefix.
     */
    public Set<String> getFileIdsForMatchingFiles(Path path) {
        var pathStr = FileUtil.pathToStringStandardSeparator(path) + "/";
        var set = new HashSet<String>();
        reverseManifestMap.forEach((contentPath, id) -> {
            if (contentPath.startsWith(pathStr)) {
                set.add(id);
            }
        });
        return set;
    }

    /**
     * If there's an active mutable HEAD, its revision id is returned. Otherwise, null is returned.
     */
    @JsonIgnore
    public RevisionId getRevisionId() {
        return revisionId;
    }

    /**
     * Indicates if there's an active mutable HEAD
     */
    public boolean hasMutableHead() {
        return mutableHead;
    }

    /**
     * The relative path from the storage root to the OCFL object directory
     */
    @JsonIgnore
    public String getObjectRootPath() {
        return objectRootPath;
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
                ", mutableHead=" + mutableHead +
                ", revisionId=" + revisionId +
                ", objectRootPath=" + objectRootPath +
                '}';
    }

    /**
     * This builder is only intended to be used by Jackson for deserializing inventory files.
     */
    @JsonPOJOBuilder
    public static class JacksonBuilder {
        String id;
        InventoryType type;
        DigestAlgorithm digestAlgorithm;
        VersionId head;
        String contentDirectory;
        Map<DigestAlgorithm, Map<String, Set<String>>> fixity;
        Map<String, Set<String>> manifest;
        Map<VersionId, Version> versions;

        boolean mutableHead;
        RevisionId revisionId;
        String objectRootPath;

        public void withId(String id) {
            this.id = id;
        }

        public void withType(InventoryType type) {
            this.type = type;
        }

        public void withDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
            this.digestAlgorithm = digestAlgorithm;
        }

        public void withHead(VersionId head) {
            this.head = head;
        }

        public void withContentDirectory(String contentDirectory) {
            this.contentDirectory = contentDirectory;
        }

        public void withFixity(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
            this.fixity = fixity;
        }

        public void withManifest(Map<String, Set<String>> manifest) {
            this.manifest = manifest;
        }

        public void withVersions(Map<VersionId, Version> versions) {
            this.versions = versions;
        }

        @JacksonInject("mutableHead")
        public void withMutableHead(boolean mutableHead) {
            this.mutableHead = mutableHead;
        }

        @JacksonInject("revisionId")
        public void withRevisionId(RevisionId revisionId) {
            this.revisionId = revisionId;
        }

        @JacksonInject("objectRootPath")
        public void withObjectRootPath(String objectRootPath) {
            this.objectRootPath = objectRootPath;
        }

        public Inventory build() {
            return new Inventory(id, type, digestAlgorithm, head, contentDirectory, fixity,
                    manifest, versions, mutableHead, revisionId, objectRootPath, null);
        }

    }

}
