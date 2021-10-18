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

package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.OcflFileAlreadyExistsException;
import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.exception.OcflNoSuchFileException;
import edu.wisc.library.ocfl.api.exception.OcflStateException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.api.model.ValidationResults;
import edu.wisc.library.ocfl.api.model.VersionNum;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionNum;
import edu.wisc.library.ocfl.core.path.constraint.LogicalPathConstraints;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.core.validation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static edu.wisc.library.ocfl.api.OcflConstants.INVENTORY_SIDECAR_PREFIX;

// TODO
public class DefaultOcflStorage extends AbstractOcflStorage {

    // TODO listing type Other

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflStorage.class);

    private static final String MEDIA_TYPE_TEXT = "text/plain; charset=UTF-8";
    private static final String MEDIA_TYPE_JSON = "application/json; charset=UTF-8";

    private final PathConstraintProcessor logicalPathConstraints;
    private final FileSystem fileSystem;
    private final OcflStorageInitializer initializer;
    private OcflStorageLayoutExtension storageLayoutExtension;
    private final Validator validator;
    private final boolean verifyInventoryDigest;

    /**
     * Create a new builder.
     *
     * @return builder
     */
    public static OcflStorageBuilder builder() {
        return new OcflStorageBuilder();
    }

    /**
     * Creates a new DefaultOcflStorage object.
     *
     * <p>{@link #initializeStorage} must be called before using this object.
     *
     * @see OcflStorageBuilder
     *
     * @param fileSystem the abstraction over the underlying filesystem that contains the OCFL repository
     * @param verifyInventoryDigest true if inventory digests should be verified on read
     * @param initializer initializes a new OCFL repo
     */
    public DefaultOcflStorage(FileSystem fileSystem,
                              boolean verifyInventoryDigest,
                              OcflStorageInitializer initializer) {
        this.fileSystem = Enforce.notNull(fileSystem, "fileSystem cannot be null");
        this.verifyInventoryDigest = verifyInventoryDigest;
        this.initializer = Enforce.notNull(initializer, "initializer cannot be null");
        this.logicalPathConstraints = LogicalPathConstraints.constraintsWithBackslashCheck();
        this.validator = new Validator(fileSystem);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        LOG.debug("Load inventory for object <{}>", objectId);

        Inventory inventory = null;

        var objectRootPath = objectRootPath(objectId);
        var objectProps = examineObject(objectRootPath);

        if (objectProps.getOcflVersion() != null) {
            if (objectProps.getDigestAlgorithm() == null) {
                throw new CorruptObjectException(String.format("Object %s is missing its root sidecar file", objectId));
            }

            var hasMutableHead = false;
            if (objectProps.hasExtensions()) {
                hasMutableHead = loadObjectExtensions(objectRootPath).contains(OcflConstants.MUTABLE_HEAD_EXT_NAME);
            }

            if (hasMutableHead) {
                inventory = parseAndVerifyMutableInventory(objectId, objectProps.getDigestAlgorithm(), objectRootPath);
                ensureRootObjectHasNotChanged(inventory);
            } else {
                inventory = parseAndVerifyInventory(objectId, objectProps.getDigestAlgorithm(), objectRootPath);
            }

            if (!Objects.equals(objectId, inventory.getId())) {
                throw new CorruptObjectException(String.format("Expected object at %s to have id %s. Found: %s",
                        objectRootPath, objectId, inventory.getId()));
            }
        }

        return inventory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public byte[] getInventoryBytes(String objectId, VersionNum versionNum) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");
        Enforce.notNull(versionNum, "versionNum cannot be null");

        LOG.debug("Loading inventory bytes for object {} version {}", objectId, versionNum);

        var objectRootPath = objectRootPath(objectId);
        var versionPath = FileUtil.pathJoinFailEmpty(objectRootPath, versionNum.toString());

        var inventoryPath = ObjectPaths.inventoryPath(versionPath);

        try (var stream = fileSystem.read(inventoryPath)) {
            return stream.readAllBytes();
        } catch (OcflNoSuchFileException e) {
            var mutableHeadInventoryPath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);

            try (var mutableStream = fileSystem.read(mutableHeadInventoryPath)) {
                var bytes = mutableStream.readAllBytes();
                var inv = inventoryMapper.readMutableHeadNoDigest("root",
                        RevisionNum.R1, new ByteArrayInputStream(bytes));

                if (versionNum.equals(inv.getHead())) {
                    return bytes;
                }
            } catch (OcflNoSuchFileException e2) {
                // Ignore missing mutable head
            } catch (IOException e2) {
                throw new OcflIOException(e2);
            }
        } catch (IOException e) {
            throw new OcflIOException(e);
        }

        throw new NotFoundException(String.format("No inventory could be found for object %s version %s", objectId, versionNum));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<String> listObjectIds() {
        LOG.debug("List object ids");

        return findOcflObjectRootDirs().map(objectRoot -> {
            return parseInventory(objectRoot).getId();
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

        LOG.debug("Store new version of object <{}> version <{}> revision <{}> from staging directory <{}>",
                inventory.getId(), inventory.getHead(), inventory.getRevisionNum(), stagingDir);

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
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionNum versionNum) {
        ensureOpen();

        LOG.debug("Get file streams for object <{}> version <{}>", inventory.getId(), versionNum);

        var version = inventory.ensureVersion(versionNum);
        var algorithm = inventory.getDigestAlgorithm();

        var map = new HashMap<String, OcflFileRetriever>(version.getState().size());

        version.getState().forEach((digest, paths) -> {
            var srcPath = inventory.storagePath(digest);
            paths.forEach(path -> {
                map.put(path, fileSystem.readLazy(srcPath, algorithm, digest));
            });
        });

        return map;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionNum versionNum, Path stagingDir) {
        ensureOpen();

        LOG.debug("Reconstruct object <{}> version <{}> in directory <{}>", inventory.getId(), versionNum, stagingDir);

        var version = inventory.ensureVersion(versionNum);
        var digestAlgorithm = inventory.getDigestAlgorithm();

        version.getState().forEach((id, files) -> {
            var srcPath = inventory.storagePath(id);

            for (var logicalPath : files) {
                logicalPathConstraints.apply(logicalPath);
                var destination = Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), logicalPath));

                UncheckedFiles.createDirectories(destination.getParent());

                try (var stream = new FixityCheckInputStream(new BufferedInputStream(fileSystem.read(srcPath)),
                        digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (FixityCheckException e) {
                    throw new FixityCheckException(
                            String.format("File %s in object %s failed its fixity check.", logicalPath, inventory.getId()), e);
                } catch (IOException e) {
                    throw OcflIOException.from(e);
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

        var objectRoot = objectRootPath(objectId);

        try {
            fileSystem.deleteDirectory(objectRoot);
        } catch (RuntimeException e) {
            throw new CorruptObjectException(String.format("Failed to purge object %s at %s. The object may need to be deleted manually.",
                    objectId, objectRoot), e);
        }

        try {
            fileSystem.deleteEmptyDirsUp(FileUtil.parentPath(objectRoot));
        } catch (RuntimeException e) {
            LOG.warn("Failed to cleanup parent directories when purging object {}.", objectId, e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollbackToVersion(Inventory inventory, VersionNum versionNum) {
        ensureOpen();

        LOG.info("Rollback object <{}> to version {}", inventory.getId(), versionNum);

        var versionPath = objectVersionPath(inventory, versionNum);

        try {
            copyInventoryInternal(inventory, versionPath, inventory.getObjectRootPath());
        } catch (Exception e) {
            try {
                var previousVersionPath = objectVersionPath(inventory, inventory.getHead());
                copyInventoryInternal(inventory, previousVersionPath, inventory.getObjectRootPath());
            } catch (RuntimeException e1) {
                LOG.error("Failed to rollback inventory at {}. Object {} must be fixed manually.",
                        ObjectPaths.inventoryPath(inventory.getObjectRootPath()), inventory.getId(), e1);
            }
            throw e;
        }

        try {
            var currentVersion = inventory.getHead();

            while (currentVersion.compareTo(versionNum) > 0) {
                LOG.info("Purging object {} version {}", inventory.getId(), currentVersion);
                fileSystem.deleteDirectory(objectVersionPath(inventory, currentVersion));
                currentVersion = currentVersion.previousVersionNum();
            }

            purgeMutableHead(inventory.getId());
        } catch (RuntimeException e) {
            throw new CorruptObjectException(String.format("Object %s was corrupted while attempting to rollback to version %s. It must be manually remediated.",
                    inventory.getId(), versionNum), e);
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

        if (!hasMutableHead(newInventory.getObjectRootPath())) {
            throw new ObjectOutOfSyncException(
                    String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", newInventory.getId()));
        }

        var versionPath = objectVersionPath(newInventory, newInventory.getHead());

        moveMutableHeadToVersionDirectory(newInventory, versionPath);

        try {
            try {
                // The inventory is written to the root first so that the mutable version can be recovered if the write fails
                copyInventoryToRootWithRollback(newInventory, stagingDir);
            } catch (RuntimeException e) {
                // TODO rollback
                throw e;
            }

            try {
                copyInventoryInternal(newInventory, newInventory.getObjectRootPath(), versionPath);
            } catch (RuntimeException e) {
                // TODO I think this is fine to log and continue
            }

            // TODO note that the FS impl cleans empty dirs here -- not sure if it's necessary or just conservative
        } catch (RuntimeException e) {
            try {
                fileSystem.deleteDirectory(versionPath);
                // TODO copy old root inv back?
            } catch (RuntimeException exception) {
                LOG.error("Failed to rollback new version installation in object {} at {}. It must be cleaned up manually.",
                        newInventory.getId(), newInventory.getHead(), e);
            }
            throw e;
        }

        try {
            purgeMutableHead(newInventory.getId());
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup mutable HEAD of object {} at {}. It must be deleted manually.",
                    newInventory.getId(), ObjectPaths.mutableHeadExtensionRoot(newInventory.getObjectRootPath()), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        LOG.info("Purge mutable HEAD on object <{}>", objectId);

        var extensionRoot = ObjectPaths.mutableHeadExtensionRoot(objectRootPath(objectId));

        try {
            fileSystem.deleteDirectory(extensionRoot);
        } catch (RuntimeException e) {
            throw new CorruptObjectException(String.format("Failed to purge mutable HEAD of object %s at %s. The version may need to be deleted manually.",
                    objectId, extensionRoot), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        var objectProps = examineObject(objectRootPath(objectId));
        var exists = objectProps.getOcflVersion() != null;

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

        Enforce.notNull(objectVersionId.getVersionNum(), "versionNum cannot be null");

        if (!containsObject(objectVersionId.getObjectId())) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectVersionId.getObjectId(), objectVersionId.getVersionNum()));
        }

        var versionRootPath = FileUtil.pathJoinFailEmpty(objectRootPath(objectVersionId.getObjectId()),
                objectVersionId.getVersionNum().toString());

        LOG.debug("Copying <{}> to <{}>", versionRootPath, outputPath);

        fileSystem.copyDirectoryOutOf(versionRootPath, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportObject(String objectId, Path outputPath) {
        ensureOpen();

        if (!containsObject(objectId)) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }

        var objectRootPath = objectRootPath(objectId);

        LOG.debug("Copying <{}> to <{}>", objectRootPath, outputPath);

        fileSystem.copyDirectoryOutOf(objectRootPath, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void importObject(String objectId, Path objectPath) {
        ensureOpen();

        var objectRootPath = objectRootPath(objectId);

        LOG.debug("Importing <{}> to <{}>", objectId, objectRootPath);

        fileSystem.createDirectories(objectRootPath);

        try {
            fileSystem.moveDirectoryInto(objectPath, objectRootPath);
        } catch (OcflFileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(String.format("Cannot import object %s because the object already exists.",
                    objectId));
        } catch (RuntimeException e) {
            try {
                purgeObject(objectId);
            } catch (RuntimeException e1) {
                LOG.error("Failed to rollback object {} import", objectId, e1);
            }
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ValidationResults validateObject(String objectId, boolean contentFixityCheck) {
        ensureOpen();

        if (!containsObject(objectId)) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }

        var objectRoot = objectRootPath(objectId);

        LOG.debug("Validating object <{}> at <{}>", objectId, objectRoot);

        return validator.validateObject(objectRoot, contentFixityCheck);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInitialize(OcflExtensionConfig layoutConfig) {
        this.storageLayoutExtension = this.initializer.initializeStorage(ocflVersion, layoutConfig, supportEvaluator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing " + this.getClass().getName());
    }

    private void storeNewImmutableVersion(Inventory inventory, Path stagingDir) {
        var objectRootPath = inventory.getObjectRootPath();
        ensureNoMutableHead(inventory.getId(), objectRootPath);

        var versionPath = objectVersionPath(inventory, inventory.getHead());
        var isFirstVersion = isFirstVersion(inventory);

        try {
            if (isFirstVersion) {
                fileSystem.createDirectories(objectRootPath);
                writeObjectNamasteFile(objectRootPath);
            }

            moveToVersionDirectory(inventory, stagingDir, versionPath);

            try {
                verifyPriorInventory(inventory, ObjectPaths.inventorySidecarPath(objectRootPath, inventory));
                copyInventoryToRootWithRollback(inventory, versionPath);
            } catch (RuntimeException e) {
                // TODO helper for these catches?
                try {
                    fileSystem.deleteDirectory(versionPath);
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback the creation of object {} version {}. The object may be corrupted.",
                            inventory.getId(), inventory.getHead(), e1);
                }
                throw e;
            }
        } catch (RuntimeException e) {
            // TODO this could corrupt the object if another process is concurrently creating the same object
            if (isFirstVersion) {
                try {
                    purgeObject(inventory.getId());
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback object {} creation", inventory.getId(), e1);
                }
            }
            // TODO wrap exception?
            throw e;
        }
    }

    private void storeNewMutableHeadVersion(Inventory inventory, Path stagingDir) {
        var destinationDir = ObjectPaths.mutableHeadCurrentRevisionContentPath(inventory);

        var isNewMutableHead = false;

        if (hasMutableHead(inventory.getObjectRootPath())) {
            ensureRootObjectHasNotChanged(inventory);
        } else {
            copyRootInventorySidecarToMutableHead(inventory);
            isNewMutableHead = true;
        }

        String revisionMarker = null;

        try {
            revisionMarker = createRevisionMarker(inventory);

            moveToRevisionDirectory(inventory, stagingDir, destinationDir);

            try {
                verifyPriorInventoryMutable(inventory, isNewMutableHead);
                // TODO if this fails the inventory may be left in a bad state
                storeMutableHeadInventory(inventory, stagingDir);
            } catch (RuntimeException e) {
                try {
                    fileSystem.deleteDirectory(destinationDir);
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback the creation of object {} version {} revision {}. The object may be corrupted.",
                            inventory.getId(), inventory.getHead(), inventory.getRevisionNum(), e1);
                }
                throw e;
            }
        } catch (RuntimeException e) {
            if (isNewMutableHead) {
                try {
                    fileSystem.deleteDirectory(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()));
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback the creation of a new mutable HEAD in object {}", inventory.getId(), e1);
                }
            } else if (revisionMarker != null) {
                try {
                    fileSystem.deleteFile(revisionMarker);
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback mutable HEAD revision marker in object {}", inventory.getId(), e1);
                }
            }
            // TODO wrap exception here?
            throw e;
        }

        try {
            deleteMutableHeadFilesNotInManifest(inventory);
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup outdated mutable HEAD content files in object {}", inventory.getId(), e);
        }

        try {
            fileSystem.deleteEmptyDirsDown(ObjectPaths.mutableHeadContentPath(inventory));
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup empty mutable HEAD content directories in object {}", inventory.getId(), e);
        }
    }

    private void moveToRevisionDirectory(Inventory inventory,
                                         Path stagingDir,
                                         String destination) {
        fileSystem.createDirectories(ObjectPaths.mutableHeadContentPath(inventory));
        try {
            fileSystem.moveDirectoryInto(ObjectPaths.version(inventory, stagingDir).contentRoot().headRevisionPath(),
                    destination);
        } catch (OcflFileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void moveToVersionDirectory(Inventory inventory,
                                        Path stagingDir,
                                        String destination) {
        try {
            fileSystem.moveDirectoryInto(stagingDir, destination);
        } catch (OcflFileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void moveMutableHeadToVersionDirectory(Inventory inventory,
                                                   String destination) {
        try {
            fileSystem.moveDirectoryInternal(ObjectPaths.mutableHeadVersionPath(inventory.getObjectRootPath()),
                    destination);
        } catch (OcflFileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private void storeMutableHeadInventory(Inventory inventory, Path sourcePath) {
        // TODO retry?
        fileSystem.copyFileInto(ObjectPaths.inventoryPath(sourcePath),
                ObjectPaths.mutableHeadInventoryPath(inventory.getObjectRootPath()), MEDIA_TYPE_JSON);
        fileSystem.copyFileInto(ObjectPaths.inventorySidecarPath(sourcePath, inventory),
                ObjectPaths.mutableHeadInventorySidecarPath(inventory.getObjectRootPath(), inventory), MEDIA_TYPE_TEXT);
    }

    private void copyInventoryToRootWithRollback(Inventory inventory, Path stagingDir) {
        try {
            // TODO retry?
            fileSystem.copyFileInto(ObjectPaths.inventoryPath(stagingDir),
                    ObjectPaths.inventoryPath(inventory.getObjectRootPath()), MEDIA_TYPE_JSON);
            fileSystem.copyFileInto(ObjectPaths.inventorySidecarPath(stagingDir, inventory),
                    ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory), MEDIA_TYPE_TEXT);
        } catch (RuntimeException e) {
            if (!isFirstVersion(inventory)) {
                rollbackInventory(inventory);
            }
            throw e;
        }
    }

    private void copyInventoryToRootWithRollback(Inventory inventory, String versionPath) {
        try {
            copyInventoryInternal(inventory, versionPath, inventory.getObjectRootPath());
        } catch (RuntimeException e) {
            rollbackInventory(inventory);
            throw e;
        }
    }

    private void rollbackInventory(Inventory inventory) {
        if (!isFirstVersion(inventory)) {
            try {
                var previousVersionPath = objectVersionPath(inventory, inventory.getHead().previousVersionNum());
                copyInventoryInternal(inventory, previousVersionPath, inventory.getObjectRootPath());
            } catch (RuntimeException e) {
                LOG.error("Failed to rollback inventory at {} in object {}. Object must be fixed manually.",
                        ObjectPaths.inventoryPath(inventory.getObjectRootPath()), inventory.getId(), e);
            }
        }
    }

    private void copyInventoryInternal(Inventory inventory, String sourcePath, String destinationPath) {
        // TODO retry?
        fileSystem.copyFileInternal(ObjectPaths.inventoryPath(sourcePath),
                ObjectPaths.inventoryPath(destinationPath));
        // TODO this does not work if there was an algorithm change and rolling back
        fileSystem.copyFileInternal(ObjectPaths.inventorySidecarPath(sourcePath, inventory),
                ObjectPaths.inventorySidecarPath(destinationPath, inventory));
    }

    private void copyRootInventorySidecarToMutableHead(Inventory inventory) {
        var rootSidecarPath = ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory);
        var sidecarName = rootSidecarPath.substring(rootSidecarPath.lastIndexOf('/') + 1);
        var destination = FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()), "root-" + sidecarName);
        fileSystem.copyFileInternal(rootSidecarPath, destination);
    }

    private void verifyPriorInventoryMutable(Inventory inventory, boolean isNewMutableHead) {
        String sidecarPath;

        if (isNewMutableHead) {
            sidecarPath = ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory);
        } else {
            sidecarPath = ObjectPaths.mutableHeadInventorySidecarPath(inventory.getObjectRootPath(), inventory);
        }

        verifyPriorInventory(inventory, sidecarPath);
    }

    private void verifyPriorInventory(Inventory inventory, String sidecarPath) {
        if (inventory.getPreviousDigest() != null) {
            var actualDigest = getDigestFromSidecar(sidecarPath);
            if (!actualDigest.equalsIgnoreCase(inventory.getPreviousDigest())) {
                throw new ObjectOutOfSyncException(String.format("Cannot update object %s because the update is out of sync with the current object state. " +
                                "The digest of the current inventory is %s, but the digest %s was expected.",
                        inventory.getId(), actualDigest, inventory.getPreviousDigest()));
            }
        } else if (!inventory.getHead().equals(VersionNum.V1)) {
            LOG.debug("Cannot verify prior inventory for object {} because its digest is unknown.", inventory.getId());
        }
    }

    private Inventory parseAndVerifyInventory(String objectId, DigestAlgorithm digestAlgorithm, String objectRootPath) {
        var remotePath = ObjectPaths.inventoryPath(objectRootPath);

        try (var stream = fileSystem.read(remotePath)) {
            var inventory = inventoryMapper.read(objectRootPath, digestAlgorithm, stream);

            if (verifyInventoryDigest) {
                var expectedDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(objectRootPath, inventory));
                if (!expectedDigest.equalsIgnoreCase(inventory.getInventoryDigest())) {
                    throw new CorruptObjectException(String.format("Invalid root inventory in object %s", objectId));
                }
            }

            return inventory;
        } catch (OcflNoSuchFileException e) {
            throw new CorruptObjectException(String.format("Object %s is missing its root inventory", objectId), e);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private Inventory parseAndVerifyMutableInventory(String objectId, DigestAlgorithm digestAlgorithm, String objectRootPath) {
        var remotePath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);

        try (var stream = fileSystem.read(remotePath)) {
            var revisionNum = identifyLatestRevision(objectRootPath);
            var inventory = inventoryMapper.readMutableHead(objectRootPath, revisionNum, digestAlgorithm, stream);

            if (verifyInventoryDigest) {
                var expectedDigest = getDigestFromSidecar(ObjectPaths.mutableHeadInventorySidecarPath(objectRootPath, inventory));
                if (!expectedDigest.equalsIgnoreCase(inventory.getInventoryDigest())) {
                    throw new CorruptObjectException(String.format("Invalid mutable HEAD inventory in object %s", objectId));
                }
            }

            return inventory;
        } catch (OcflNoSuchFileException e) {
            throw new CorruptObjectException(String.format("Object %s is missing its mutable HEAD inventory", objectId), e);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private Inventory parseInventory(String objectRootPath) {
        var inventoryPath = ObjectPaths.inventoryPath(objectRootPath);
        try (var stream = fileSystem.read(inventoryPath)) {
            return inventoryMapper.readNoDigest(objectRootPath, stream);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private String createRevisionMarker(Inventory inventory) {
        var revision = inventory.getRevisionNum().toString();
        var revisionsDir = ObjectPaths.mutableHeadRevisionsPath(inventory.getObjectRootPath());
        var revisionPath = FileUtil.pathJoinFailEmpty(revisionsDir, revision);

        fileSystem.createDirectories(revisionsDir);
        try {
            fileSystem.write(revisionPath, revision.getBytes(StandardCharsets.UTF_8), MEDIA_TYPE_TEXT);
            return revisionPath;
        } catch (OcflFileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        }
    }

    private RevisionNum identifyLatestRevision(String objectRootPath) {
        var revisionsPath = ObjectPaths.mutableHeadRevisionsPath(objectRootPath);
        List<Listing> revisions;

        try {
            revisions = fileSystem.listDirectory(revisionsPath);
        } catch (OcflNoSuchFileException e) {
            throw new CorruptObjectException("Object has a mutable head, but has not specified any revision numbers.", e);
        }

        var result = revisions.stream().filter(Listing::isFile)
                .map(Listing::getRelativePath)
                .filter(RevisionNum::isRevisionNum)
                .map(RevisionNum::fromString)
                .max(Comparator.naturalOrder());

        if (result.isEmpty()) {
            throw new CorruptObjectException("Object has a mutable head, but has not specified any revision numbers.");
        }

        return result.get();
    }

    private void deleteMutableHeadFilesNotInManifest(Inventory inventory) {
        // TODO see about not constructing the same paths so many times
        var files = fileSystem.listRecursive(ObjectPaths.mutableHeadContentPath(inventory));
        var prefix = FileUtil.pathJoinFailEmpty(OcflConstants.MUTABLE_HEAD_VERSION_PATH, inventory.resolveContentDirectory());

        var toDelete = files.stream().filter(Listing::isFile)
                .map(file -> FileUtil.pathJoinFailEmpty(prefix, file.getRelativePath()))
                .filter(contentPath -> inventory.getFileId(contentPath) == null)
                .collect(Collectors.toList());

        fileSystem.deleteFiles(toDelete);
    }

    private Stream<String> findOcflObjectRootDirs() {
        var iterator = fileSystem.iterateObjects();
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

    private void ensureNoMutableHead(String objectId, String objectRootPath) {
        if (hasMutableHead(objectRootPath)) {
            throw new OcflStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    objectId));
        }
    }

    private boolean hasMutableHead(String objectRootPath) {
        return fileSystem.fileExists(ObjectPaths.inventoryPath(ObjectPaths.mutableHeadVersionPath(objectRootPath)));
    }

    private void ensureRootObjectHasNotChanged(Inventory inventory) {
        var savedDigest = getDigestFromSidecar(FileUtil.pathJoinFailEmpty(ObjectPaths.mutableHeadExtensionRoot(inventory.getObjectRootPath()),
                "root-" + INVENTORY_SIDECAR_PREFIX + inventory.getDigestAlgorithm().getOcflName()));
        var rootDigest = getDigestFromSidecar(ObjectPaths.inventorySidecarPath(inventory.getObjectRootPath(), inventory));

        if (!savedDigest.equalsIgnoreCase(rootDigest)) {
            throw new ObjectOutOfSyncException(
                    String.format("The mutable HEAD of object %s is out of sync with the root object state.", inventory.getId()));
        }
    }

    private String getDigestFromSidecar(String sidecarPath) {
        try {
            var sidecarContents = fileSystem.readToString(sidecarPath);
            var parts = sidecarContents.split("\\s");
            if (parts.length == 0) {
                throw new CorruptObjectException("Invalid inventory sidecar file: " + sidecarPath);
            }
            return parts[0];
        } catch (OcflNoSuchFileException e) {
            throw new CorruptObjectException("Missing inventory sidecar: " + sidecarPath, e);
        }
    }

    private String objectVersionPath(Inventory inventory, VersionNum versionNum) {
        return FileUtil.pathJoinFailEmpty(inventory.getObjectRootPath(), versionNum.toString());
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private void writeObjectNamasteFile(String objectRootPath) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflObjectVersion());
        var namastePath = FileUtil.pathJoinFailEmpty(objectRootPath, namasteFile.fileName());
        fileSystem.write(namastePath,
                namasteFile.fileContent().getBytes(StandardCharsets.UTF_8),
                MEDIA_TYPE_TEXT);
    }

    private ObjectProperties examineObject(String objectRootPath) {
        var properties = new ObjectProperties();
        List<Listing> files;

        try {
            files = fileSystem.listDirectory(objectRootPath);
        } catch (OcflNoSuchFileException e) {
            return properties;
        }

        for (var file : files) {
            if (file.isFile()) {
                if (file.getRelativePath().startsWith(OcflConstants.INVENTORY_SIDECAR_PREFIX)) {
                    properties.setDigestAlgorithm(SidecarMapper.getDigestAlgorithmFromSidecar(file.getRelativePath()));
                } else if (file.getRelativePath().startsWith(OcflConstants.OBJECT_NAMASTE_PREFIX)) {
                    properties.setOcflVersion(OcflVersion.fromOcflObjectVersionFilename(file.getRelativePath()));
                }
            } else {
                if (OcflConstants.EXTENSIONS_DIR.equals(file.getRelativePath())) {
                    properties.setExtensions(true);
                }
            }
            if (properties.getOcflVersion() != null
                    && properties.getDigestAlgorithm() != null
                    && properties.hasExtensions()) {
                break;
            }
        }

        return properties;
    }

    /**
     * @return a set of supported extension names
     */
    private Set<String> loadObjectExtensions(String objectRoot) {
        // Currently, this just ensures that the object does not use any extensions that ocfl-java does not support
        try {
            return fileSystem.listDirectory(ObjectPaths.extensionsPath(objectRoot)).stream()
                    .filter(Listing::isDirectory)
                    .map(Listing::getRelativePath)
                    .filter(supportEvaluator::checkSupport)
                    .collect(Collectors.toSet());
        } catch (OcflNoSuchFileException e) {
            return Collections.emptySet();
        }
    }

}
