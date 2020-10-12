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

package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JacksonInject;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.InventoryType;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.nio.file.Path;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * OCFL inventory object. It is intended to be used to encode and decode inventories. Inventories are immutable. Creating
 * a new version of an object requires creating a new inventory.
 *
 * @see InventoryBuilder
 * @see <a href="https://ocfl.io/">https://ocfl.io/</a>
 */
@JsonDeserialize(builder = Inventory.JacksonBuilder.class)
@JsonPropertyOrder({
        "id",
        "type",
        "digestAlgorithm",
        "head",
        "contentDirectory",
        "fixity",
        "manifest",
        "versions"
})
public class Inventory {

    private final String id;
    private final InventoryType type;
    private final DigestAlgorithm digestAlgorithm;
    private final VersionId head;
    private final String contentDirectory;

    @JsonIgnore
    private final Map<DigestAlgorithm, PathBiMap> fixityBiMap;

    @JsonIgnore
    private final PathBiMap manifestBiMap;

    private final Map<VersionId, Version> versions;

    // This property is injected
    @JsonIgnore
    private final RevisionId revisionId;

    // This property is injected
    @JsonIgnore
    private final boolean mutableHead;

    // This property is injected
    @JsonIgnore
    private final String objectRootPath;

    /**
     * Creates a stub inventory that is useful when creating new objects. It should NOT be persisted.
     *
     * @param id object id
     * @param config ocfl defaults config
     * @param objectRootPath path to object root
     * @return stub inventory
     */
    public static Inventory stubInventory(
            String id,
            OcflConfig config,
            String objectRootPath) {
        return new Inventory(id, config.getOcflVersion().getInventoryType(),
                config.getDefaultDigestAlgorithm(),
                config.getDefaultContentDirectory(), objectRootPath);
    }

    /**
     * @return new {@link InventoryBuilder} that is not based on an existing inventory
     */
    public static InventoryBuilder builder() {
        return new InventoryBuilder();
    }

    /**
     * @return new {@link InventoryBuilder} that copies values from an existing inventory
     */
    public static InventoryBuilder builder(Inventory original) {
        return new InventoryBuilder(original);
    }

    /**
     * Creates an inventory builder with values populated from a stub inventory
     *
     * @param id object id
     * @param config ocfl defaults config
     * @param objectRootPath path to object root
     * @return inventory builder
     */
    public static InventoryBuilder builderFromStub(String id, OcflConfig config, String objectRootPath) {
        return new InventoryBuilder(Inventory.stubInventory(id, config, objectRootPath));
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
            String objectRootPath) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        this.type = Enforce.notNull(type, "type cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        Enforce.expressionTrue(OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(digestAlgorithm), digestAlgorithm,
                "digestAlgorithm must be sha512 or sha256");
        this.head = Enforce.notNull(head, "head cannot be null");
        this.contentDirectory = contentDirectory;
        this.fixityBiMap = createFixityBiMap(fixity);
        this.manifestBiMap = PathBiMap.fromFileIdMap(manifest);
        var tree = new TreeMap<VersionId, Version>(Comparator.naturalOrder());
        tree.putAll(versions);
        this.versions = Collections.unmodifiableMap(tree);

        this.mutableHead = mutableHead;
        this.revisionId = revisionId;
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be blank");
    }

    /**
     * Creates a stub inventory that contains nothing. This is useful when building new objects.
     */
    private Inventory(
            String id,
            InventoryType type,
            DigestAlgorithm digestAlgorithm,
            String contentDirectory,
            String objectRootPath) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        this.type = Enforce.notNull(type, "type cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.head = new VersionId(0);
        this.contentDirectory = contentDirectory;
        this.fixityBiMap = Collections.emptyMap();
        this.manifestBiMap = new PathBiMap();
        this.versions = Collections.emptyMap();

        this.mutableHead = false;
        this.revisionId = null;
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be null");
    }

    private static Map<DigestAlgorithm, PathBiMap> createFixityBiMap(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
        var map = new HashMap<DigestAlgorithm, PathBiMap>();

        fixity.forEach((algorithm, values) -> {
            map.put(algorithm, PathBiMap.fromFileIdMap(values));
        });

        return Collections.unmodifiableMap(map);
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
        var fixity = new HashMap<DigestAlgorithm, Map<String, Set<String>>>();

        fixityBiMap.forEach((algorithm, map) -> {
            fixity.put(algorithm, map.getFileIdToPaths());
        });

        return fixity;
    }

    /**
     * A map of all of the files that are part of the object across all versions of the object. The map is keyed off file
     * digest and the value is the location of the file relative to the OCFL object root.
     */
    @JsonGetter("manifest")
    public Map<String, Set<String>> getManifest() {
        return manifestBiMap.getFileIdToPaths();
    }

    /**
     * A map of version identifiers to the object that describes the state of the object at that version. All versions of
     * the object are represented here.
     */
    @JsonGetter("versions")
    public Map<VersionId, Version> getVersions() {
        return versions;
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
     * Returns the Version that corresponds to the versionId. Throws an exception if the version does not exist.
     *
     * @param versionId version id of the version to retrieve
     * @return the version
     * @throws IllegalStateException if the version does not exist
     */
    public Version ensureVersion(VersionId versionId) {
        var version = getVersion(versionId);

        if (version == null) {
            throw new IllegalStateException(String.format("Object %s does not contain version %s", id, versionId));
        }

        return version;
    }

    /**
     * Helper method for checking if an object contains a file with the given digest id.
     */
    public boolean manifestContainsFileId(String fileId) {
        return manifestBiMap.containsFileId(fileId);
    }

    /**
     * Returns the digest that is used to identify the given path if it exists.
     */
    public String getFileId(String path) {
        return manifestBiMap.getFileId(path);
    }

    /**
     * Returns the digest that is used to identify the given path if it exists.
     */
    public String getFileId(Path path) {
        return manifestBiMap.getFileId(FileUtil.pathToStringStandardSeparator(path));
    }

    /**
     * Returns the set of paths that are identified by the given digest if they exist.
     */
    public Set<String> getContentPaths(String fileId) {
        return manifestBiMap.getPaths(fileId);
    }

    /**
     * Returns the first path to a file that maps to the given digest
     */
    public String getContentPath(String fileId) {
        var paths = manifestBiMap.getPaths(fileId);
        if (paths.isEmpty()) {
            return null;
        }
        return paths.iterator().next();
    }

    /**
     * Returns the first contentPath associated to a fileId. Throws an exception if there is no mapping.
     *
     * @param fileId the fileId to lookup
     * @return the mapped content path
     * @throws IllegalStateException if there is no mapping
     */
    public String ensureContentPath(String fileId) {
        if (!manifestContainsFileId(fileId)) {
            throw new IllegalStateException(String.format("Missing manifest entry for %s in object %s.",
                    fileId, id));
        }
        return getContentPath(fileId);
    }

    /**
     * Returns the path from the storage root to a file within an object.
     *
     * @param fileId the fileId of the file to lookup
     * @return the path from the storage root to the file
     */
    public String storagePath(String fileId) {
        return FileUtil.pathJoinFailEmpty(objectRootPath, ensureContentPath(fileId));
    }

    /**
     * Returns the set of file ids of files that have content paths that begin with the given prefix.
     */
    public Set<String> getFileIdsForMatchingFiles(Path path) {
        return getFileIdsForMatchingFiles(FileUtil.pathToStringStandardSeparator(path));
    }

    /**
     * Returns the set of file ids of files that have content paths that begin with the given prefix.
     */
    public Set<String> getFileIdsForMatchingFiles(String path) {
        var pathStr = path + "/";
        var set = new HashSet<String>();
        manifestBiMap.getPathToFileId().forEach((contentPath, id) -> {
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

    /**
     * Returns the next version id after the current HEAD version. If the object has a mutable HEAD, the current version
     * is returned.
     *
     * @return the next version id
     */
    public VersionId nextVersionId() {
        if (mutableHead) {
            return head;
        }
        return head.nextVersionId();
    }

    /**
     * Returns the next revision id. If the object doest not have a revision id, then a new revision is created.
     *
     * @return the next revision id
     */
    public RevisionId nextRevisionId() {
        if (revisionId == null) {
            return new RevisionId(1);
        }
        return revisionId.nextRevisionId();
    }

    /**
     * Returns the fixity information for a contentPath.
     *
     * @param contentPath the content path
     * @return fixity information or empty map
     */
    public Map<DigestAlgorithm, String> getFixityForContentPath(String contentPath) {
        var fixity = new HashMap<DigestAlgorithm, String>();

        fixityBiMap.forEach((algorithm, map) -> {
            if (map.containsPath(contentPath)) {
                fixity.put(algorithm, map.getFileId(contentPath));
            }
        });

        return fixity;
    }

    @Override
    public String toString() {
        return "Inventory{" +
                "id='" + id + '\'' +
                ", type='" + type + '\'' +
                ", digestAlgorithm='" + digestAlgorithm + '\'' +
                ", head='" + head + '\'' +
                ", contentDirectory='" + contentDirectory + '\'' +
                ", fixity=" + fixityBiMap +
                ", manifest=" + manifestBiMap +
                ", versions=" + versions +
                ", mutableHead=" + mutableHead +
                ", revisionId=" + revisionId +
                ", objectRootPath=" + objectRootPath +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Inventory inventory = (Inventory) o;
        return mutableHead == inventory.mutableHead &&
                id.equals(inventory.id) &&
                type == inventory.type &&
                digestAlgorithm.equals(inventory.digestAlgorithm) &&
                head.equals(inventory.head) &&
                Objects.equals(contentDirectory, inventory.contentDirectory) &&
                fixityBiMap.equals(inventory.fixityBiMap) &&
                manifestBiMap.equals(inventory.manifestBiMap) &&
                versions.equals(inventory.versions) &&
                Objects.equals(revisionId, inventory.revisionId) &&
                objectRootPath.equals(inventory.objectRootPath);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, type, digestAlgorithm, head, contentDirectory, fixityBiMap, manifestBiMap, versions, revisionId, mutableHead, objectRootPath);
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
                    manifest, versions, mutableHead, revisionId, objectRootPath);
        }

    }

}
