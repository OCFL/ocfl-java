package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;

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

    private boolean mutableHead;
    private RevisionId revisionId;
    private String objectRootPath;

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
        this.mutableHead = original.hasMutableHead();
        this.revisionId = original.getRevisionId();
        this.objectRootPath = original.getObjectRootPath();
        this.fixity = original.getMutableFixity();
        this.manifest = original.getMutableManifest();
        this.versions = original.getMutableVersions();
        this.reverseManifestMap = original.getMutableReverseManifestMap();
    }

    public Set<String> getContentPaths(String fileId) {
        return manifest.getOrDefault(fileId, new HashSet<>());
    }

    public InventoryBuilder addNewHeadVersion(VersionId versionId, Version version) {
        Enforce.notNull(versionId, "versionId cannot be null");
        Enforce.notNull(version, "version cannot be null");

        versions.put(versionId, version);
        head = versionId;
        return this;
    }

    public InventoryBuilder addFileToManifest(String id, String contentPath) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(contentPath, "contentPath cannot be blank");

        manifest.computeIfAbsent(id, k -> new HashSet<>()).add(contentPath);
        reverseManifestMap.put(contentPath, id);
        return this;
    }

    public InventoryBuilder removeFileFromManifest(String contentPath) {
        var digest = reverseManifestMap.remove(contentPath);
        if (digest != null) {
            var paths = manifest.get(digest);
            if (paths.size() == 1) {
                manifest.remove(digest);
            } else {
                paths.remove(contentPath);
            }

            removeFileFromFixity(contentPath);
        }
        return this;
    }

    public InventoryBuilder removeFileFromFixity(String contentPath) {
        fixity.forEach((algorithm, values) -> {
            for (var it = values.entrySet().iterator(); it.hasNext();) {
                var set = it.next();
                if (set.getValue().contains(contentPath)) {
                    if (set.getValue().size() == 1) {
                        it.remove();
                    } else {
                        set.getValue().remove(contentPath);
                    }
                    break;
                }
            }
        });

        return this;
    }

    public InventoryBuilder addFixityForFile(String contentPath, DigestAlgorithm algorithm, String value) {
        Enforce.notBlank(contentPath, "contentPath cannot be blank");
        Enforce.notNull(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be blank");

        fixity.computeIfAbsent(algorithm, k -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER))
                .computeIfAbsent(value, k -> new HashSet<>()).add(contentPath);
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

    public InventoryBuilder mutableHead(boolean mutableHead) {
        this.mutableHead = mutableHead;
        return this;
    }

    public InventoryBuilder revisionId(RevisionId revisionId) {
        this.revisionId = revisionId;
        return this;
    }

    public InventoryBuilder objectRootPath(String objectRootPath) {
        this.objectRootPath = objectRootPath;
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
        if (contentDirectory == null) {
            return OcflConstants.DEFAULT_CONTENT_DIRECTORY;
        }
        return contentDirectory;
    }

    public String getObjectRootPath() {
        return objectRootPath;
    }

    public String getVersionFileId(VersionId versionId, String path) {
        var version = versions.get(versionId);

        if (version == null) {
            throw new IllegalStateException(String.format("Version %s does not exist for object %s.", versionId, id));
        }

        return version.getFileId(path);
    }

    public Inventory build() {
        return new Inventory(id, type, digestAlgorithm, head, contentDirectory,
                fixity, manifest, versions, mutableHead, revisionId, objectRootPath, reverseManifestMap);
    }

}
