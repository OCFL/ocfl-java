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

package io.ocfl.core.model;

import io.ocfl.api.exception.OcflInputException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.InventoryType;
import io.ocfl.api.model.VersionNum;
import io.ocfl.api.util.Enforce;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Used to construct Inventory objects.
 */
public class InventoryBuilder {

    private static final VersionNum INITIAL_VERSION = VersionNum.V1;
    private static final RevisionNum INITIAL_REVISION = RevisionNum.R1;

    private String id;
    private InventoryType type;
    private DigestAlgorithm digestAlgorithm;
    private VersionNum head;
    private String contentDirectory;

    private boolean mutableHead;
    private RevisionNum revisionNum;
    private String objectRootPath;

    private Map<DigestAlgorithm, PathBiMap> fixity;
    private PathBiMap manifest;
    private Map<VersionNum, Version> versions;

    private VersionNum nextHeadVersion;

    private String previousDigest;
    private String inventoryDigest;

    public InventoryBuilder() {
        fixity = new HashMap<>();
        manifest = new PathBiMap();
        versions = new HashMap<>();
        nextHeadVersion = INITIAL_VERSION;
    }

    /**
     * Used to construct a new {@link Inventory} that's based on an existing inventory. This should be used when creating a new
     * version of an existing object.
     *
     * @param original the original inventory
     */
    public InventoryBuilder(Inventory original) {
        Enforce.notNull(original, "inventory cannot be null");
        this.id = original.getId();
        this.type = original.getType();
        this.digestAlgorithm = original.getDigestAlgorithm();
        this.head = original.getHead();
        this.contentDirectory = original.getContentDirectory();
        this.mutableHead = original.hasMutableHead();
        this.revisionNum = original.getRevisionNum();
        this.objectRootPath = original.getObjectRootPath();
        this.fixity = fixityToBiMap(original.getFixity());
        this.manifest = PathBiMap.fromFileIdMap(original.getManifest());
        this.versions = new HashMap<>(original.getVersions());
        this.previousDigest = original.getPreviousDigest();
        this.inventoryDigest = original.getInventoryDigest();

        this.nextHeadVersion = head.nextVersionNum();
    }

    private static Map<DigestAlgorithm, PathBiMap> fixityToBiMap(
            Map<DigestAlgorithm, Map<String, Set<String>>> originalFixity) {
        var fixity = new HashMap<DigestAlgorithm, PathBiMap>();

        originalFixity.forEach(((digestAlgorithm, map) -> {
            fixity.put(digestAlgorithm, PathBiMap.fromFileIdMap(map));
        }));

        return fixity;
    }

    /**
     * Add the version as the new HEAD version. This assigns the version the next available version number, unless it's mutable.
     *
     * @param version the new version
     * @return builder
     */
    public InventoryBuilder addHeadVersion(Version version) {
        Enforce.notNull(version, "version cannot be null");

        if (mutableHead) {
            if (revisionNum == null) {
                revisionNum = INITIAL_REVISION;
                head = nextHeadVersion;
            } else {
                revisionNum = revisionNum.nextRevisionNum();
            }
            versions.put(head, version);
        } else {
            versions.put(nextHeadVersion, version);
            head = nextHeadVersion;
            nextHeadVersion = nextHeadVersion.nextVersionNum();
        }

        return this;
    }

    /**
     * Inserts a version at the specified version number. This will OVERWRITE any version that is currently at that location.
     *
     * @param versionNum the id of the version
     * @param version the version
     * @return builder
     */
    public InventoryBuilder putVersion(VersionNum versionNum, Version version) {
        Enforce.notNull(versionNum, "versionNum cannot be null");
        Enforce.notNull(version, "version cannot be null");

        versions.put(versionNum, version);
        return this;
    }

    /**
     * Adds a file to the manifest
     *
     * @param id the fileId of the file
     * @param contentPath the content path of the file
     * @return builder
     */
    public InventoryBuilder addFileToManifest(String id, String contentPath) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(contentPath, "contentPath cannot be blank");

        manifest.put(id, contentPath);
        return this;
    }

    /**
     * Removes a file from the manifest and fixity block
     *
     * @param fileId the fileId of the file
     * @return builder
     */
    public InventoryBuilder removeFileId(String fileId) {
        var paths = manifest.removeFileId(fileId);
        if (paths != null) {
            paths.forEach(this::removeContentPathFromFixity);
        }

        return this;
    }

    /**
     * Removes a file from the manifest and fixity bock by content path
     *
     * @param contentPath the content path of the file to remove
     * @return builder
     */
    public InventoryBuilder removeContentPath(String contentPath) {
        Enforce.notBlank(contentPath, "contentPath cannot be blank");

        manifest.removePath(contentPath);
        removeContentPathFromFixity(contentPath);
        return this;
    }

    /**
     * Removes a file from the fixity block by content path
     *
     * @param contentPath the content path of the file to remove
     * @return builder
     */
    public InventoryBuilder removeContentPathFromFixity(String contentPath) {
        Enforce.notBlank(contentPath, "contentPath cannot be blank");

        fixity.values().forEach(map -> map.removePath(contentPath));
        return this;
    }

    /**
     * Adds a file to the fixity block
     *
     * @param contentPath the content path of the file
     * @param algorithm digest algorithm
     * @param value digest value
     * @return builder
     */
    public InventoryBuilder addFixityForFile(String contentPath, DigestAlgorithm algorithm, String value) {
        Enforce.notBlank(contentPath, "contentPath cannot be blank");
        Enforce.notNull(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be blank");

        if (!manifest.containsPath(contentPath)) {
            throw new OcflInputException(String.format(
                    "Cannot add fixity information for content path %s because it is not present in the manifest.",
                    contentPath));
        }

        fixity.computeIfAbsent(algorithm, k -> new PathBiMap()).put(value, contentPath);
        return this;
    }

    /**
     * Removes all of the entries from the fixity block.
     *
     * @return builder
     */
    public InventoryBuilder clearFixity() {
        fixity.clear();
        return this;
    }

    public InventoryBuilder id(String id) {
        this.id = Enforce.notBlank(id, "id cannot be blank");
        return this;
    }

    public InventoryBuilder type(InventoryType type) {
        this.type = Enforce.notNull(type, "type cannot be null");
        return this;
    }

    public InventoryBuilder digestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        return this;
    }

    public InventoryBuilder head(VersionNum head) {
        this.head = Enforce.notNull(head, "head cannot be null");
        this.nextHeadVersion = head.nextVersionNum();
        return this;
    }

    public InventoryBuilder contentDirectory(String contentDirectory) {
        this.contentDirectory = contentDirectory;
        return this;
    }

    public InventoryBuilder fixityBiMap(Map<DigestAlgorithm, PathBiMap> fixity) {
        this.fixity = Enforce.notNull(fixity, "fixity cannot be null");
        return this;
    }

    public InventoryBuilder fixity(Map<DigestAlgorithm, Map<String, Set<String>>> fixity) {
        this.fixity = fixityToBiMap(fixity == null ? Collections.emptyMap() : fixity);
        return this;
    }

    public InventoryBuilder manifest(PathBiMap manifest) {
        this.manifest = Enforce.notNull(manifest, "manifest cannot be null");
        return this;
    }

    public InventoryBuilder manifest(Map<String, Set<String>> manifest) {
        this.manifest = PathBiMap.fromFileIdMap(Enforce.notNull(manifest, "manifest cannot be null"));
        return this;
    }

    public InventoryBuilder versions(Map<VersionNum, Version> versions) {
        this.versions = Enforce.notNull(versions, "versions cannot be null");
        return this;
    }

    public InventoryBuilder mutableHead(boolean mutableHead) {
        this.mutableHead = mutableHead;
        return this;
    }

    public InventoryBuilder revisionNum(RevisionNum revisionNum) {
        this.revisionNum = revisionNum;
        return this;
    }

    public InventoryBuilder objectRootPath(String objectRootPath) {
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be blank");
        return this;
    }

    public InventoryBuilder previousDigest(String previousDigest) {
        this.previousDigest = previousDigest;
        return this;
    }

    public InventoryBuilder inventoryDigest(String inventoryDigest) {
        this.inventoryDigest = inventoryDigest;
        return this;
    }

    /**
     * @return a new Inventory
     */
    public Inventory build() {
        return new Inventory(
                id,
                type,
                digestAlgorithm,
                head,
                contentDirectory,
                fixityFromBiMap(),
                manifest.getFileIdToPaths(),
                versions,
                mutableHead,
                revisionNum,
                objectRootPath,
                previousDigest,
                inventoryDigest);
    }

    /**
     * Indicates if the manifest contains a fileId
     *
     * @param fileId the fileId
     * @return true if the manifest contains the fileId
     */
    public boolean containsFileId(String fileId) {
        return manifest.containsFileId(fileId);
    }

    /**
     * Indicates if the manifest contains a contentPath
     *
     * @param contentPath the contentPath
     * @return true if the manifest contains the contentPath
     */
    public boolean containsContentPath(String contentPath) {
        return manifest.containsPath(contentPath);
    }

    /**
     * Retrieves the content paths associated to the fileId or an empty set
     *
     * @param fileId the fileId
     * @return associated content paths or an empty set
     */
    public Set<String> getContentPaths(String fileId) {
        return manifest.getPaths(fileId);
    }

    /**
     * Retrieves the fileId associated to the content path
     *
     * @param contentPath the contentPath
     * @return associated fileId or null
     */
    public String getFileId(String contentPath) {
        return manifest.getFileId(contentPath);
    }

    public boolean hasMutableHead() {
        return mutableHead;
    }

    /**
     * Returns the fixity digest for a file or null
     *
     * @param fileId the fileId
     * @param algorithm the digest algorithm
     * @return digest or null
     */
    public String getFileFixity(String fileId, DigestAlgorithm algorithm) {
        var contentPaths = manifest.getPaths(fileId);

        for (var contentPath : contentPaths) {
            var fixityMap = fixity.get(algorithm);
            if (fixityMap != null) {
                var digest = fixityMap.getFileId(contentPath);
                if (digest != null) {
                    return digest;
                }
            }
        }

        return null;
    }

    /**
     * @return the inventory's type
     */
    public InventoryType getType() {
        return type;
    }

    private Map<DigestAlgorithm, Map<String, Set<String>>> fixityFromBiMap() {
        var transformed = new HashMap<DigestAlgorithm, Map<String, Set<String>>>();

        fixity.forEach((algorithm, map) -> {
            transformed.put(algorithm, map.getFileIdToPaths());
        });

        return transformed;
    }
}
