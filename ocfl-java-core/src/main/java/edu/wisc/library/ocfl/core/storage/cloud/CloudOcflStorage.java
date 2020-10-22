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

package edu.wisc.library.ocfl.core.storage.cloud;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.LogicalPathConstraints;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.storage.AbstractOcflStorage;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * {@link OcflStorage} implementation for integrating with cloud storage providers. {@link CloudClient} implementation
 * to integrate with different providers.
 */
public class CloudOcflStorage extends AbstractOcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(CloudOcflStorage.class);

    private static final String MIMETYPE_TEXT_PLAIN = "text/plain; charset=UTF-8";

    private final PathConstraintProcessor logicalPathConstraints;

    private final CloudClient cloudClient;
    private final Path workDir;

    private final CloudOcflStorageInitializer initializer;
    private OcflStorageLayoutExtension storageLayoutExtension;
    private final CloudOcflFileRetriever.Builder fileRetrieverBuilder;

    /**
     * Create a new builder.
     *
     * @return builder
     */
    public static CloudOcflStorageBuilder builder() {
        return new CloudOcflStorageBuilder();
    }

    /**
     * Creates a new CloudOcflStorage object.
     *
     * <p>{@link #initializeStorage} must be called before using this object.
     *
     * @see CloudOcflStorageBuilder
     *
     * @param cloudClient the client to use to interface with cloud storage such as S3
     * @param initializer initializes a new OCFL repo
     */
    public CloudOcflStorage(CloudClient cloudClient, Path workDir, CloudOcflStorageInitializer initializer) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.initializer = Enforce.notNull(initializer, "initializer cannot be null");

        this.logicalPathConstraints = LogicalPathConstraints.constraintsWithBackslashCheck();
        this.fileRetrieverBuilder = CloudOcflFileRetriever.builder().cloudClient(this.cloudClient);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        LOG.debug("Load inventory for object <{}>", objectId);

        var objectRootPath = objectRootPath(objectId);
        var tempDir = FileUtil.createObjectTempDir(workDir, objectId);
        var localInventoryPath = tempDir.resolve(OcflConstants.INVENTORY_FILE);

        try {
            var inventory = downloadAndVerifyMutableInventory(objectRootPath, localInventoryPath);

            if (inventory != null) {
                ensureRootObjectHasNotChanged(inventory);
            } else {
                inventory = downloadAndVerifyInventory(objectRootPath, localInventoryPath);
            }

            return inventory;
        } finally {
            FileUtil.safeDeleteDirectory(tempDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<String> listObjectIds() {
        LOG.debug("List object ids");

        return findOcflObjectRootDirs().map(objectRoot -> {
            var inventory = downloadInventory(objectRoot);
            return inventory.getId();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

        LOG.debug("Store new version of object <{}> version <{}> revision <{}> from staging directory <{}>",
                inventory.getId(), inventory.getHead(), inventory.getRevisionId(), stagingDir);

        if (inventory.hasMutableHead()) {
            storeNewMutableHeadVersion(inventory, stagingDir);
        } else {
            storeNewImmutableVersion(inventory, stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        ensureOpen();

        LOG.debug("Get file streams for object <{}> version <{}>", inventory.getId(), versionId);

        var version = inventory.ensureVersion(versionId);
        var algorithm = inventory.getDigestAlgorithm();

        var map = new HashMap<String, OcflFileRetriever>(version.getState().size());

        version.getState().forEach((digest, paths) -> {
            var srcPath = inventory.storagePath(digest);
            paths.forEach(path -> {
                map.put(path, fileRetrieverBuilder.build(srcPath, algorithm, digest));
            });
        });

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {
        ensureOpen();

        LOG.debug("Reconstruct object <{}> version <{}> in directory <{}>", inventory.getId(), versionId, stagingDir);

        var version = inventory.ensureVersion(versionId);
        var digestAlgorithm = inventory.getDigestAlgorithm();

        version.getState().forEach((id, files) -> {
            var srcPath = inventory.storagePath(id);

            for (var logicalPath : files) {
                logicalPathConstraints.apply(logicalPath);
                var destination = Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), logicalPath));

                UncheckedFiles.createDirectories(destination.getParent());

                try (var stream = new FixityCheckInputStream(cloudClient.downloadStream(srcPath), digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (FixityCheckException e) {
                    throw new FixityCheckException(
                            String.format("File %s in object %s failed its fixity check.", logicalPath, inventory.getId()), e);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
        ensureOpen();

        LOG.info("Purge object <{}>", objectId);

        cloudClient.deletePath(objectRootPath(objectId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollbackToVersion(Inventory inventory, VersionId versionId) {
        ensureOpen();

        LOG.info("Rollback object <{}> to version {}", inventory.getId(), versionId);

        var versionPath = objectVersionPath(inventory, versionId);

        try {
            copyInventoryToRoot(versionPath, inventory);
        } catch (Exception e) {
            try {
                var previousVersionPath = objectVersionPath(inventory, inventory.getHead());
                copyInventoryToRoot(previousVersionPath, inventory);
            } catch (RuntimeException e1) {
                LOG.error("Failed to rollback inventory at {}. Object must be fixed manually.",
                        ObjectPaths.inventoryPath(inventory.getObjectRootPath()), e1);
            }
        }

        try {
            var currentVersion = inventory.getHead();

            while (currentVersion.compareTo(versionId) > 0) {
                LOG.info("Purging object {} version {}", inventory.getId(), currentVersion);
                cloudClient.deletePath(objectVersionPath(inventory, currentVersion));
                currentVersion = currentVersion.previousVersionId();
            }

            purgeMutableHead(inventory.getId());
        } catch (Exception e) {
            throw new CorruptObjectException(String.format("Object %s was corrupted while attempting to rollback to version %s. It must be manually remediated.",
                    inventory.getId(), versionId), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir) {
        ensureOpen();

        LOG.debug("Commit mutable HEAD on object <{}>", newInventory.getId());

        ensureRootObjectHasNotChanged(newInventory);

        if (cloudClient.listDirectory(ObjectPaths.mutableHeadVersionPath(newInventory.getObjectRootPath())).getObjects().isEmpty()) {
            throw new ObjectOutOfSyncException(
                    String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", newInventory.getId()));
        }

        var versionPath = objectVersionPath(newInventory, newInventory.getHead());

        ensureVersionDoesNotExist(newInventory, versionPath);

        var objectKeys = copyMutableVersionToImmutableVersion(oldInventory, newInventory);

        try {
            storeInventoryInCloudWithRollback(newInventory, stagingDir, versionPath);

            try {
                purgeMutableHead(newInventory.getId());
            } catch (RuntimeException e) {
                LOG.error("Failed to cleanup mutable HEAD of object {} at {}. It must be deleted manually.",
                        newInventory.getId(), ObjectPaths.mutableHeadExtensionRoot(newInventory.getObjectRootPath()), e);
            }
        } catch (RuntimeException e) {
            cloudClient.safeDeleteObjects(objectKeys);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        LOG.info("Purge mutable HEAD on object <{}>", objectId);

        cloudClient.deletePath(ObjectPaths.mutableHeadExtensionRoot(objectRootPath(objectId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        var exists = !cloudClient.listDirectory(objectRootPath(objectId)).getObjects().isEmpty();

        LOG.debug("OCFL repository contains object <{}>: {}", objectId, exists);

        return exists;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String objectRootPath(String objectId) {
        ensureOpen();

        var objectRootPath = storageLayoutExtension.mapObjectId(objectId);

        LOG.debug("Object root path for object <{}>: {}", objectId, objectRootPath);

        return objectRootPath;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportVersion(ObjectVersionId objectVersionId, Path outputPath) {
        ensureOpen();

        Enforce.notNull(objectVersionId.getVersionId(), "versionId cannot be null");

        var versionRootPath = FileUtil.pathJoinFailEmpty(objectRootPath(objectVersionId.getObjectId()),
                objectVersionId.getVersionId().toString()) + "/";

        var objects = cloudClient.list(versionRootPath).getObjects();

        if (objects.isEmpty()) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectVersionId.getObjectId(), objectVersionId.getVersionId()));
        }

        LOG.debug("Copying <{}> to <{}>", versionRootPath, outputPath);

        copyObjects(objects, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportObject(String objectId, Path outputPath) {
        ensureOpen();

        var objectRootPath = objectRootPath(objectId) + "/";

        var objects = cloudClient.list(objectRootPath).getObjects();

        if (objects.isEmpty()) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }

        LOG.debug("Copying <{}> to <{}>", objectRootPath, outputPath);

        copyObjects(objects, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void importObject(String objectId, Path objectPath) {
        var objectRootPath = objectRootPath(objectId);

        if (!cloudClient.listDirectory(objectRootPath).getObjects().isEmpty()) {
            throw new ObjectOutOfSyncException(String.format("Cannot import object %s because the object already exists.",
                    objectId));
        }

        LOG.debug("Importing <{}> to <{}>", objectId, objectRootPath);

        storeFilesInCloud(objectPath, objectRootPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInitialize(OcflExtensionConfig layoutConfig) {
        this.storageLayoutExtension = this.initializer.initializeStorage(ocflVersion, layoutConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing " + this.getClass().getName());
    }

    private void storeNewImmutableVersion(Inventory inventory, Path stagingDir) {
        ensureNoMutableHead(inventory);

        var versionPath = objectVersionPath(inventory, inventory.getHead());
        ensureVersionDoesNotExist(inventory, versionPath);

        String namasteFile = null;

        try {
            if (isFirstVersion(inventory)) {
                namasteFile = writeObjectNamasteFile(inventory.getObjectRootPath());
            }

            var objectKeys = storeContentInCloud(inventory, stagingDir);
            // TODO write a copy to the cache?

            try {
                storeInventoryInCloudWithRollback(inventory, stagingDir, versionPath);
            } catch (RuntimeException e) {
                cloudClient.safeDeleteObjects(objectKeys);
                throw e;
            }
        } catch (RuntimeException e) {
            // TODO this could corrupt the object if another process is concurrently creating the same object
            if (namasteFile != null) {
                cloudClient.safeDeleteObjects(namasteFile);
            }
            throw e;
        }
    }

    private void storeNewMutableHeadVersion(Inventory inventory, Path stagingDir) {
        ensureRevisionDoesNotExist(inventory);

        var cleanupKeys = new ArrayList<String>(2);

        if (!cloudClient.listDirectory(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath())).getObjects().isEmpty()) {
            ensureRootObjectHasNotChanged(inventory);
        } else {
            cleanupKeys.add(copyRootInventorySidecarToMutableHead(inventory));
        }

        try {
            cleanupKeys.add(createRevisionMarker(inventory));

            var objectKeys = storeContentInCloud(inventory, stagingDir);

            // TODO write a copy to the cache?

            try {
                // TODO if this fails the inventory may be left in a bad state
                storeMutableHeadInventoryInCloud(inventory, stagingDir);
            } catch (RuntimeException e) {
                cloudClient.safeDeleteObjects(objectKeys);
                throw e;
            }
        } catch (RuntimeException e) {
            cloudClient.safeDeleteObjects(cleanupKeys);
            throw e;
        }

        deleteMutableHeadFilesNotInManifest(inventory);
    }

    private List<String> storeContentInCloud(Inventory inventory, Path sourcePath) {
        var contentPrefix = contentPrefix(inventory);
        var fileIds = inventory.getFileIdsForMatchingFiles(contentPrefix);
        var objectKeys = Collections.synchronizedList(new ArrayList<String>());

        try {
            fileIds.forEach(fileId -> {
                var contentPath = inventory.ensureContentPath(fileId);
                var contentPathNoVersion = contentPath.substring(contentPath.indexOf(inventory.resolveContentDirectory()));
                var file = sourcePath.resolve(contentPathNoVersion);

                if (Files.notExists(file)) {
                    throw new IllegalStateException(String.format("Staged file %s does not exist", file));
                }

                var key = inventory.storagePath(fileId);
                objectKeys.add(key);
                cloudClient.uploadFile(file, key);
            });
        } catch (RuntimeException e) {
            cloudClient.safeDeleteObjects(objectKeys);
            throw e;
        }

        return objectKeys;
    }

    private List<String> storeFilesInCloud(Path source, String destination) {
        var objectKeys = Collections.synchronizedList(new ArrayList<String>());

        try (var paths = Files.walk(source)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                var relative = FileUtil.pathToStringStandardSeparator(source.relativize(file));
                var key = FileUtil.pathJoinFailEmpty(destination, relative);
                objectKeys.add(key);
                cloudClient.uploadFile(file, key);
            });
        } catch (IOException | RuntimeException e) {
            cloudClient.safeDeleteObjects(objectKeys);

            if (e instanceof IOException) {
                throw new UncheckedIOException((IOException) e);
            }
            throw (RuntimeException) e;
        }

        return objectKeys;
    }

    private List<String> copyMutableVersionToImmutableVersion(Inventory oldInventory, Inventory newInventory) {
        var contentPrefix = contentPrefix(newInventory);
        var fileIds = newInventory.getFileIdsForMatchingFiles(contentPrefix);
        var objectKeys = Collections.synchronizedList(new ArrayList<String>());

        try {
            // TODO the performance here DOES improve with increased parallelization -- perhaps add in the future
            fileIds.forEach(fileId -> {
                var srcPath = oldInventory.storagePath(fileId);
                var dstPath = newInventory.storagePath(fileId);
                objectKeys.add(dstPath);
                cloudClient.copyObject(srcPath, dstPath);
            });
        } catch (RuntimeException e) {
            cloudClient.safeDeleteObjects(objectKeys);
            throw e;
        }

        return objectKeys;
    }

    private void storeMutableHeadInventoryInCloud(Inventory inventory, Path sourcePath) {
        cloudClient.uploadFile(ObjectPaths.inventoryPath(sourcePath),
                ObjectPaths.mutableHeadInventoryPath(inventory.getObjectRootPath()));
        cloudClient.uploadFile(ObjectPaths.inventorySidecarPath(sourcePath, inventory),
                ObjectPaths.mutableHeadInventorySidecarPath(inventory.getObjectRootPath(), inventory));
    }

    private void storeInventoryInCloudWithRollback(Inventory inventory, Path sourcePath, String versionPath) {
        var srcInventoryPath = ObjectPaths.inventoryPath(sourcePath);
        var srcSidecarPath = ObjectPaths.inventorySidecarPath(sourcePath, inventory);
        var versionedInventoryPath = ObjectPaths.inventoryPath(versionPath);
        var versionedSidecarPath = ObjectPaths.inventorySidecarPath(versionPath, inventory);

        cloudClient.uploadFile(srcInventoryPath, versionedInventoryPath);
        cloudClient.uploadFile(srcSidecarPath, versionedSidecarPath);

        try {
            copyInventoryToRoot(versionPath, inventory);
        } catch (RuntimeException e) {
            rollbackInventory(inventory);
            cloudClient.safeDeleteObjects(versionedInventoryPath, versionedSidecarPath);
            throw e;
        }
    }

    private void rollbackInventory(Inventory inventory) {
        if (!isFirstVersion(inventory)) {
            try {
                var previousVersionPath = objectVersionPath(inventory, inventory.getHead().previousVersionId());
                copyInventoryToRoot(previousVersionPath, inventory);
            } catch (RuntimeException e) {
                LOG.error("Failed to rollback inventory at {}. Object must be fixed manually.",
                        ObjectPaths.inventoryPath(inventory.getObjectRootPath()), e);
            }
        }
    }

    private void copyInventoryToRoot(String versionPath, Inventory inventory) {
        cloudClient.copyObject(ObjectPaths.inventoryPath(versionPath),
                ObjectPaths.inventoryPath(inventory.getObjectRootPath()));
        cloudClient.copyObject(ObjectPaths.inventorySidecarPath(versionPath, inventory),
                ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory));
    }

    private String copyRootInventorySidecarToMutableHead(Inventory inventory) {
        var rootSidecarPath = ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory);
        var sidecarName = rootSidecarPath.substring(rootSidecarPath.lastIndexOf('/') + 1);
        return cloudClient.copyObject(rootSidecarPath,
                FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()), "root-" + sidecarName)).getPath();
    }

    private Inventory downloadAndVerifyInventory(String objectRootPath, Path localPath) {
        try {
            var remotePath = ObjectPaths.inventoryPath(objectRootPath);
            cloudClient.downloadFile(remotePath, localPath);
            var inventory = inventoryMapper.read(objectRootPath, localPath);
            var expectedDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(objectRootPath, inventory));
            return verifyInventory(expectedDigest, localPath, inventory);
        } catch (KeyNotFoundException e) {
            // Doesn't exist; return null
            return null;
        }
    }

    private Inventory downloadAndVerifyMutableInventory(String objectRootPath, Path localPath) {
        try {
            var remotePath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);
            cloudClient.downloadFile(remotePath, localPath);
            var revisionId = identifyLatestRevision(objectRootPath);
            var inventory = inventoryMapper.readMutableHead(objectRootPath, revisionId, localPath);
            var expectedDigest = getDigestFromSidecar(ObjectPaths.mutableHeadInventorySidecarPath(objectRootPath, inventory));
            return verifyInventory(expectedDigest, localPath, inventory);
        } catch (KeyNotFoundException e) {
            // Doesn't exist; return null
            return null;
        }
    }

    private Inventory downloadInventory(String objectRootPath) {
        var inventoryPath = ObjectPaths.inventoryPath(objectRootPath);
        try (var stream = cloudClient.downloadStream(inventoryPath)) {
            return inventoryMapper.read(objectRootPath, stream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String createRevisionMarker(Inventory inventory) {
        var revision = inventory.getRevisionId().toString();
        var revisionPath = FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadRevisionsPath(inventory.getObjectRootPath()), revision);
        return cloudClient.uploadBytes(revisionPath, revision.getBytes(StandardCharsets.UTF_8), MIMETYPE_TEXT_PLAIN).getPath();
    }

    private RevisionId identifyLatestRevision(String objectRootPath) {
        var revisionsPath = ObjectPaths.mutableHeadRevisionsPath(objectRootPath);
        var revisions = cloudClient.listDirectory(revisionsPath);

        RevisionId revisionId = null;

        for (var revisionStr : revisions.getObjects()) {
            var id = RevisionId.fromString(revisionStr.getKeySuffix());
            if (revisionId == null) {
                revisionId = id;
            } else if (revisionId.compareTo(id) < 1) {
                revisionId = id;
            }
        }

        return revisionId;
    }

    private void deleteMutableHeadFilesNotInManifest(Inventory inventory) {
        var keys = cloudClient.list(FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadVersionPath(inventory.getObjectRootPath()),
                inventory.resolveContentDirectory()));
        var deleteKeys = new ArrayList<String>();

        keys.getObjects().forEach(o -> {
            var key = o.getKey().getPath();
            var contentPath = key.substring(inventory.getObjectRootPath().length() + 1);
            if (inventory.getFileId(contentPath) == null) {
                deleteKeys.add(key);
            }
        });

        cloudClient.safeDeleteObjects(deleteKeys);
    }

    private String contentPrefix(Inventory inventory) {
        if (inventory.hasMutableHead()) {
            return FileUtil.pathJoinFailEmpty(
                    OcflConstants.MUTABLE_HEAD_VERSION_PATH,
                    inventory.resolveContentDirectory(),
                    inventory.getRevisionId().toString());
        }

        return inventory.getHead().toString();
    }

    private Stream<String> findOcflObjectRootDirs() {
        var iterator = new CloudOcflObjectRootDirIterator("", cloudClient);
        try {
            var spliterator = Spliterators.spliteratorUnknownSize(iterator,
                    Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.DISTINCT);
            return StreamSupport.stream(spliterator, false)
                    .onClose(iterator::close);
        } catch (RuntimeException e) {
            iterator.close();
            throw e;
        }
    }

    private void ensureNoMutableHead(Inventory inventory) {
        // TODO this could be incorrect due to eventual consistency issues
        if (!cloudClient.listDirectory(ObjectPaths.mutableHeadVersionPath(inventory.getObjectRootPath())).getObjects().isEmpty()) {
            // TODO modeled exception?
            throw new IllegalStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    inventory.getId()));
        }
    }

    private void ensureVersionDoesNotExist(Inventory inventory, String versionPath) {
        if (!cloudClient.listDirectory(versionPath).getObjects().isEmpty()) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void ensureRevisionDoesNotExist(Inventory inventory) {
        var latestRevision = identifyLatestRevision(inventory.getObjectRootPath());
        if (latestRevision != null && latestRevision.compareTo(inventory.getRevisionId()) >= 0) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void ensureRootObjectHasNotChanged(Inventory inventory) {
        var savedDigest = getDigestFromSidecar(FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()),
                "root-" + OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getOcflName()));
        var rootDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory));

        if (!savedDigest.equalsIgnoreCase(rootDigest)) {
            throw new ObjectOutOfSyncException(
                    String.format("The mutable HEAD of object %s is out of sync with the root object state.", inventory.getId()));
        }
    }

    private String getDigestFromSidecar(String sidecarPath) {
        try {
            var sidecarContents = cloudClient.downloadString(sidecarPath);
            var parts = sidecarContents.split("\\s");
            if (parts.length == 0) {
                throw new CorruptObjectException("Invalid inventory sidecar file: " + sidecarPath);
            }
            return parts[0];
        } catch (KeyNotFoundException e) {
            throw new CorruptObjectException("Missing inventory sidecar: " + sidecarPath, e);
        }
    }

    private Inventory verifyInventory(String expectedDigest, Path inventoryPath, Inventory inventory) {
        var algorithm = inventory.getDigestAlgorithm();
        var actualDigest = DigestUtil.computeDigestHex(algorithm, inventoryPath);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new CorruptObjectException(String.format("Invalid inventory file: %s. Expected %s digest: %s; Actual: %s",
                    inventoryPath, algorithm.getOcflName(), expectedDigest, actualDigest));
        }

        return inventory;
    }

    private String objectVersionPath(Inventory inventory, VersionId versionId) {
        return FileUtil.pathJoinFailEmpty(inventory.getObjectRootPath(), versionId.toString());
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private String writeObjectNamasteFile(String objectRootPath) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflObjectVersion());
        var key = FileUtil.pathJoinFailEmpty(objectRootPath, namasteFile.fileName());
        return cloudClient.uploadBytes(key, namasteFile.fileContent().getBytes(StandardCharsets.UTF_8), MIMETYPE_TEXT_PLAIN).getPath();
    }

    private void copyObjects(List<ListResult.ObjectListing> objects, Path outputPath) {
        objects.forEach(object -> {
            var key = object.getKey().getKey();
            var relativePath = object.getKeySuffix();
            var destination = outputPath.resolve(relativePath);

            UncheckedFiles.createDirectories(destination.getParent());

            try (var stream = cloudClient.downloadStream(key)) {
                Files.copy(stream, destination);
                // TODO fixity check?
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });
    }

}
