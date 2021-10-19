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

package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
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
import edu.wisc.library.ocfl.core.storage.AbstractOcflStorage;
import edu.wisc.library.ocfl.core.storage.ObjectProperties;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.core.validation.Validator;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FileSystemOcflStorage extends AbstractOcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorage.class);

    private final PathConstraintProcessor logicalPathConstraints;

    private final Path repositoryRoot;
    private final FileSystemOcflStorageInitializer initializer;
    private final Validator validator;
    private final boolean verifyInventoryDigest;

    private OcflStorageLayoutExtension storageLayoutExtension;

    /**
     * This retry policy is used for retrying IO operations
     */
    private final RetryPolicy<Void> ioRetry;

    /**
     * Create a new builder.
     *
     * @return builder
     */
    public static FileSystemOcflStorageBuilder builder() {
        return new FileSystemOcflStorageBuilder();
    }

    /**
     * Creates a new FileSystemOcflStorage object.
     *
     * <p>{@link #initializeStorage} must be called before using this object.
     *
     * @see FileSystemOcflStorageBuilder
     *
     * @param repositoryRoot OCFL repository root directory
     * @param verifyInventoryDigest true if inventory digests should be verified on read
     * @param initializer initializes a new OCFL repo
     */
    public FileSystemOcflStorage(Path repositoryRoot,
                                 boolean verifyInventoryDigest,
                                 FileSystemOcflStorageInitializer initializer) {
        this.repositoryRoot = Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        this.initializer = Enforce.notNull(initializer, "initializer cannot be null");

        this.verifyInventoryDigest = verifyInventoryDigest;
        this.logicalPathConstraints = LogicalPathConstraints.constraintsWithBackslashCheck();
        this.ioRetry = new RetryPolicy<Void>()
                .handle(UncheckedIOException.class, IOException.class)
                .withBackoff(5, 200, ChronoUnit.MILLIS, 1.5)
                .withMaxRetries(5);
        this.validator = new Validator(new LocalFileSystem(repositoryRoot));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        LOG.debug("Load inventory for object <{}>", objectId);

        Inventory inventory = null;
        var objectRootPathStr = objectRootPath(objectId);
        var objectRootPathAbsolute = repositoryRoot.resolve(objectRootPathStr);
        var objectProps = examineObject(objectRootPathAbsolute);

        if (objectProps.getOcflVersion() != null) {
            if (objectProps.getDigestAlgorithm() == null) {
                throw new CorruptObjectException(String.format("Object %s is missing its root sidecar file", objectId));
            }

            var hasMutableHead = false;
            if (objectProps.hasExtensions()) {
                hasMutableHead = loadObjectExtensions(objectRootPathAbsolute).contains(OcflConstants.MUTABLE_HEAD_EXT_NAME);
            }

            if (hasMutableHead) {
                var mutableHeadInventoryPath = ObjectPaths.mutableHeadInventoryPath(objectRootPathAbsolute);
                inventory = parseMutableHeadInventory(objectRootPathStr, objectRootPathAbsolute, objectProps.getDigestAlgorithm(), mutableHeadInventoryPath);
                ensureRootObjectHasNotChanged(objectId, objectRootPathAbsolute);
            } else {
                inventory = parseInventory(objectRootPathStr, objectProps.getDigestAlgorithm(), ObjectPaths.inventoryPath(objectRootPathAbsolute));
            }

            if (!Objects.equals(objectId, inventory.getId())) {
                throw new CorruptObjectException(String.format("Expected object at %s to have id %s. Found: %s",
                        objectRootPathStr, objectId, inventory.getId()));
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

        var objectRootPath = repositoryRoot.resolve(objectRootPath(objectId));
        var versionPath = objectRootPath.resolve(versionNum.toString());

        try {
            return Files.readAllBytes(ObjectPaths.inventoryPath(versionPath));
        } catch (NoSuchFileException e) {
            try {
                var bytes = Files.readAllBytes(ObjectPaths.mutableHeadInventoryPath(objectRootPath));
                var inv = inventoryMapper.readMutableHeadNoDigest("root",
                        RevisionNum.R1, new ByteArrayInputStream(bytes));

                if (versionNum.equals(inv.getHead())) {
                    return bytes;
                }
            } catch (NoSuchFileException e2) {
                // ignore
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

        return findOcflObjectRootDirs(repositoryRoot).map(rootPath -> {
            var relativeRootStr = FileUtil.pathToStringStandardSeparator(repositoryRoot.resolve(rootPath));
            var inventoryPath = ObjectPaths.inventoryPath(repositoryRoot.resolve(rootPath));
            var inventory = inventoryMapper.readNoDigest(relativeRootStr, inventoryPath);
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
                inventory.getId(), inventory.getHead(), inventory.getRevisionNum(), stagingDir);

        var objectRootPath = objectRootPathFull(inventory.getId());
        var objectRoot = ObjectPaths.objectRoot(inventory, objectRootPath);

        if (inventory.hasMutableHead()) {
            storeNewMutableHeadVersion(inventory, objectRoot, stagingDir);
        } else {
            storeNewImmutableVersion(inventory, objectRoot, stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionNum versionNum) {
        ensureOpen();

        LOG.debug("Get file streams for object <{}> version <{}>", inventory.getId(), versionNum);

        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = inventory.ensureVersion(versionNum);
        var algorithm = inventory.getDigestAlgorithm();

        var map = new HashMap<String, OcflFileRetriever>(version.getState().size());

        version.getState().forEach((digest, paths) -> {
            var srcPath = objectRootPath.resolve(inventory.ensureContentPath(digest));

            paths.forEach(path -> {
                map.put(path, new FileSystemOcflFileRetriever(srcPath, algorithm, digest));
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

        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = inventory.ensureVersion(versionNum);
        var digestAlgorithm = inventory.getDigestAlgorithm().getJavaStandardName();

        version.getState().forEach((id, files) -> {
            var srcPath = objectRootPath.resolve(inventory.ensureContentPath(id));

            for (var logicalPath : files) {
                logicalPathConstraints.apply(logicalPath);
                var destination = Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), logicalPath));

                UncheckedFiles.createDirectories(destination.getParent());

                try (var stream = new FixityCheckInputStream(new BufferedInputStream(Files.newInputStream(srcPath)), digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (IOException e) {
                    throw new OcflIOException(e);
                } catch (FixityCheckException e) {
                    throw new FixityCheckException(
                            String.format("File %s in object %s failed its fixity check.", logicalPath, inventory.getId()), e);
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

        var objectRootPath = objectRootPathFull(objectId);

        try {
            FileUtil.deleteDirectory(objectRootPath);
        } catch (Exception e) {
            throw new CorruptObjectException(String.format("Failed to purge object %s at %s. The object may need to be deleted manually.",
                    objectId, objectRootPath), e);
        }

        if (Files.exists(objectRootPath.getParent())) {
            try {
                FileUtil.deleteDirAndParentsIfEmpty(objectRootPath.getParent());
            } catch (OcflIOException e) {
                LOG.error(String.format("Failed to cleanup all empty directories in path %s." +
                        " There may be empty directories remaining in the OCFL storage hierarchy.", objectRootPath), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollbackToVersion(Inventory inventory, VersionNum versionNum) {
        ensureOpen();

        LOG.info("Rollback object <{}> to version {}", inventory.getId(), versionNum);

        var objectRootPath = objectRootPathFull(inventory.getId());
        var objectRoot = ObjectPaths.objectRoot(inventory, objectRootPath);

        var versionRoot = objectRoot.version(versionNum);

        try {
            copyInventory(versionRoot, objectRoot);
        } catch (Exception e) {
            try {
                copyInventory(objectRoot.headVersion(), objectRoot);
            } catch (Exception e1) {
                LOG.error("Failed to rollback inventory at {}", objectRoot.inventoryFile(), e1);
            }
            throw e;
        }

        try {
            var currentVersion = inventory.getHead();

            while (currentVersion.compareTo(versionNum) > 0) {
                LOG.info("Purging object {} version {}", inventory.getId(), currentVersion);
                var currentVersionPath = objectRoot.versionPath(currentVersion);
                FileUtil.deleteDirectory(currentVersionPath);
                currentVersion = currentVersion.previousVersionNum();
            }

            FileUtil.deleteDirectory(objectRoot.mutableHeadExtensionPath());
        } catch (Exception e) {
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

        var objectRootPath = objectRootPathFull(newInventory.getId());
        var objectRoot = ObjectPaths.objectRoot(newInventory, objectRootPath);

        ensureRootObjectHasNotChanged(newInventory, objectRoot);

        if (!Files.exists(objectRoot.mutableHeadVersion().inventoryFile())) {
            throw new ObjectOutOfSyncException(
                    String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", newInventory.getId()));
        }

        var versionRoot = objectRoot.headVersion();
        var stagingRoot = ObjectPaths.version(newInventory, stagingDir);

        // This is a safe guard to remove any remaining files that are in the new version content directory but
        // are no longer referenced in the inventory.
        deleteMutableHeadFilesNotInManifest(oldInventory, objectRoot, versionRoot);

        moveToVersionDirectory(newInventory, objectRoot.mutableHeadPath(), versionRoot);

        try {
            try {
                // The inventory is written to the root first so that the mutable version can be recovered if the write fails
                copyInventoryToRootWithRollback(stagingRoot, objectRoot, newInventory);
                copyInventory(stagingRoot, versionRoot);
            } catch (RuntimeException e) {
                try {
                    FileUtil.moveDirectory(versionRoot.path(), objectRoot.mutableHeadPath());
                } catch (RuntimeException | FileAlreadyExistsException e1) {
                    LOG.error("Failed to move {} back to {}", versionRoot.path(), objectRoot.mutableHeadPath(), e1);
                }
                throw e;
            }

            deleteEmptyDirs(versionRoot.contentPath());
        } catch (RuntimeException e) {
            FileUtil.safeDeleteDirectory(versionRoot.path());
            throw e;
        }

        try {
            FileUtil.deleteDirectory(objectRoot.mutableHeadExtensionPath());
        } catch (RuntimeException e) {
            LOG.error("Failed to cleanup mutable HEAD of object {} at {}. It must be deleted manually.",
                    newInventory.getId(), objectRoot.mutableHeadExtensionPath(), e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        LOG.info("Purge mutable HEAD on object <{}>", objectId);

        var objectRootPath = objectRootPathFull(objectId);
        var extensionRoot = ObjectPaths.mutableHeadExtensionRoot(objectRootPath);

        if (Files.exists(extensionRoot)) {
            try (var paths = Files.walk(extensionRoot)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(f -> {
                            try {
                                Files.delete(f);
                            } catch (IOException e) {
                                throw new OcflIOException(String.format("Failed to delete file %s while purging mutable HEAD of object %s." +
                                        " The purge failed and the mutable HEAD may need to be deleted manually.", f, objectId), e);
                            }
                        });
            } catch (IOException e) {
                throw new OcflIOException(String.format("Failed to purge mutable HEAD of object %s at %s. The object may need to be deleted manually.",
                        objectId, extensionRoot), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        var objectProps = examineObject(objectRootPathFull(objectId));
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

        var objectRootPath = objectRootPathFull(objectVersionId.getObjectId());

        if (Files.notExists(objectRootPath)) {
            throw new NotFoundException(String.format("Object %s was not found.", objectVersionId.getObjectId()));
        }

        var versionRoot = objectRootPath.resolve(objectVersionId.getVersionNum().toString());

        if (Files.notExists(versionRoot)) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectVersionId.getObjectId(), objectVersionId.getVersionNum()));
        }

        LOG.debug("Copying <{}> to <{}>", versionRoot, outputPath);

        FileUtil.recursiveCopy(versionRoot, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportObject(String objectId, Path outputPath) {
        ensureOpen();

        var objectRootPath = objectRootPathFull(objectId);

        if (Files.notExists(objectRootPath)) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }

        LOG.debug("Copying <{}> to <{}>", objectRootPath, outputPath);

        FileUtil.recursiveCopy(objectRootPath, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void importObject(String objectId, Path objectPath) {
        ensureOpen();

        var objectRootPath = objectRootPathFull(objectId);

        LOG.debug("Importing <{}> to <{}>", objectId, objectRootPath);

        UncheckedFiles.createDirectories(objectRootPath.getParent());

        try {
            FileUtil.moveDirectory(objectPath, objectRootPath);
        } catch (FileAlreadyExistsException e) {
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

        var objectRoot = objectRootPath(objectId);
        var objectRootPath = repositoryRoot.resolve(objectRoot);

        if (Files.notExists(objectRootPath)) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }

        LOG.debug("Validating object <{}> at <{}>", objectId, objectRoot);

        return validator.validateObject(objectRoot, contentFixityCheck);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInitialize(OcflExtensionConfig layoutConfig) {
        this.storageLayoutExtension = this.initializer.initializeStorage(repositoryRoot,
                ocflVersion,
                layoutConfig,
                supportEvaluator);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing " + this.getClass().getName());
    }

    private Path objectRootPathFull(String objectId) {
        return repositoryRoot.resolve(storageLayoutExtension.mapObjectId(objectId));
    }

    private Inventory parseInventory(String objectRootPath, DigestAlgorithm digestAlgorithm, Path inventoryPath) {
        var inventory = inventoryMapper.read(objectRootPath, digestAlgorithm, inventoryPath);

        if (verifyInventoryDigest) {
            var expectedDigest = SidecarMapper.readDigestRequired(ObjectPaths.inventorySidecarPath(inventoryPath.getParent(), inventory));
            if (!expectedDigest.equalsIgnoreCase(inventory.getInventoryDigest())) {
                throw new CorruptObjectException(String.format("Root inventory in object %s does not match expected digest", inventory.getId()));
            }
        }

        return inventory;
    }

    private Inventory parseMutableHeadInventory(String objectRootPath,
                                                Path objectRootPathAbsolute,
                                                DigestAlgorithm digestAlgorithm,
                                                Path inventoryPath) {
        var revisionNum = identifyLatestRevision(objectRootPathAbsolute);

        var inventory = inventoryMapper.readMutableHead(objectRootPath, revisionNum, digestAlgorithm, inventoryPath);

        if (verifyInventoryDigest) {
            var expectedDigest = SidecarMapper.readDigestRequired(ObjectPaths.inventorySidecarPath(inventoryPath.getParent(), inventory));
            if (!expectedDigest.equalsIgnoreCase(inventory.getInventoryDigest())) {
                throw new CorruptObjectException(String.format("Mutable HEAD inventory in object %s does not match expected digest", inventory.getId()));
            }
        }

        return inventory;
    }

    private RevisionNum identifyLatestRevision(Path objectRootPath) {
        var revisionsPath = ObjectPaths.mutableHeadRevisionsPath(objectRootPath);
        try (var files = Files.list(revisionsPath)) {
            var result = files.filter(Files::isRegularFile)
                    .map(Path::getFileName).map(Path::toString)
                    .filter(RevisionNum::isRevisionNum)
                    .map(RevisionNum::fromString)
                    .max(Comparator.naturalOrder());
            if (result.isEmpty()) {
                return null;
            }
            return result.get();
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private void storeNewImmutableVersion(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, Path stagingDir) {
        ensureNoMutableHead(objectRoot);

        var versionRoot = objectRoot.headVersion();

        var isFirstVersion = isFirstVersion(inventory);

        try {
            if (isFirstVersion) {
                setupNewObjectDirs(objectRoot.path());
            }

            moveToVersionDirectory(inventory, stagingDir, versionRoot);

            try {
                verifyPriorInventory(inventory, objectRoot.inventorySidecar());
                copyInventoryToRootWithRollback(versionRoot, objectRoot, inventory);
            } catch (RuntimeException e) {
                FileUtil.safeDeleteDirectory(versionRoot.path());
                throw e;
            }
        } catch (RuntimeException e) {
            if (isFirstVersion && Files.notExists(objectRoot.inventoryFile())) {
                try {
                    purgeObject(inventory.getId());
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback object {} creation", inventory.getId(), e1);
                }
            }
            throw e;
        }
    }

    private void storeNewMutableHeadVersion(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, Path stagingDir) {
        ensureRootObjectHasNotChanged(inventory, objectRoot);

        var versionRoot = objectRoot.headVersion();
        var revisionPath = versionRoot.contentRoot().headRevisionPath();
        var stagingVersionRoot = ObjectPaths.version(inventory, stagingDir);
        var revisionsDirPath = objectRoot.mutableHeadRevisionsPath();

        var isNewMutableHead = Files.notExists(versionRoot.inventoryFile());

        try {
            var revisionMarker = createRevisionMarker(inventory, revisionsDirPath);

            try {
                moveToRevisionDirectory(inventory, stagingVersionRoot, versionRoot);

                if (isNewMutableHead) {
                    copyRootInventorySidecar(objectRoot, versionRoot);
                }

                try {
                    verifyPriorInventoryMutable(inventory, objectRoot, isNewMutableHead);
                    copyInventory(stagingVersionRoot, versionRoot);
                } catch (RuntimeException e) {
                    FileUtil.safeDeleteDirectory(revisionPath);
                    throw e;
                }
            } catch (RuntimeException e) {
                FileUtil.safeDeleteDirectory(revisionMarker);
                throw e;
            }
        } catch (RuntimeException e) {
            if (isNewMutableHead) {
                FileUtil.safeDeleteDirectory(versionRoot.path().getParent());
            }
            throw e;
        }

        deleteEmptyDirs(versionRoot.contentPath());
        deleteMutableHeadFilesNotInManifest(inventory, objectRoot, versionRoot);
    }

    private void copyRootInventorySidecar(ObjectPaths.ObjectRoot objectRoot, ObjectPaths.VersionRoot versionRoot) {
        var rootSidecar = objectRoot.inventorySidecar();
        UncheckedFiles.copy(rootSidecar,
                versionRoot.path().getParent().resolve("root-" + rootSidecar.getFileName().toString()),
                StandardCopyOption.REPLACE_EXISTING);
    }

    private void moveToVersionDirectory(Inventory inventory, Path source, ObjectPaths.VersionRoot versionRoot) {
        try {
            Files.createDirectories(versionRoot.path().getParent());
            FileUtil.moveDirectory(source, versionRoot.path());
        } catch (FileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private Path createRevisionMarker(Inventory inventory, Path revisionsPath) {
        UncheckedFiles.createDirectories(revisionsPath);
        var revision = inventory.getRevisionNum().toString();
        try {
            var revisionMarker = Files.createFile(revisionsPath.resolve(revision));
            return Files.writeString(revisionMarker, revision);
        } catch (FileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private void moveToRevisionDirectory(Inventory inventory, ObjectPaths.VersionRoot source, ObjectPaths.VersionRoot destination) {
        try {
            Files.createDirectories(destination.contentPath());
            FileUtil.moveDirectory(source.contentRoot().headRevisionPath(), destination.contentRoot().headRevisionPath());
        } catch (FileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private boolean isFirstVersion(Inventory inventory) {
        return VersionNum.V1.equals(inventory.getHead());
    }

    private void setupNewObjectDirs(Path objectRootPath) {
        UncheckedFiles.createDirectories(objectRootPath);
        new NamasteTypeFile(ocflVersion.getOcflObjectVersion()).writeFile(objectRootPath);
    }

    private void copyInventory(ObjectPaths.HasInventory source, ObjectPaths.HasInventory destination) {
        Failsafe.with(ioRetry).run(() -> {
            LOG.debug("Copying {} to {}", source.inventoryFile(), destination.inventoryFile());
            UncheckedFiles.copy(source.inventoryFile(), destination.inventoryFile(), StandardCopyOption.REPLACE_EXISTING);
            UncheckedFiles.copy(source.inventorySidecar(), destination.inventorySidecar(), StandardCopyOption.REPLACE_EXISTING);
        });
    }

    private void copyInventoryToRootWithRollback(ObjectPaths.HasInventory source, ObjectPaths.ObjectRoot objectRoot, Inventory inventory) {
        try {
            copyInventory(source, objectRoot);
        } catch (RuntimeException e) {
            if (!isFirstVersion(inventory)) {
                try {
                    var previousVersionRoot = objectRoot.version(inventory.getHead().previousVersionNum());
                    copyInventory(previousVersionRoot, objectRoot);
                } catch (RuntimeException e1) {
                    LOG.error("Failed to rollback inventory at {}", objectRoot.inventoryFile(), e1);
                }
            }
            throw e;
        }
    }

    private void deleteEmptyDirs(Path path) {
        try {
            FileUtil.deleteEmptyDirs(path);
        } catch (RuntimeException e) {
            // This does not fail the commit
            LOG.error("Failed to delete an empty directory. It may need to be deleted manually.", e);
        }
    }

    private void deleteMutableHeadFilesNotInManifest(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, ObjectPaths.VersionRoot versionRoot) {
        var files = FileUtil.findFiles(versionRoot.contentPath());
        files.forEach(file -> {
            if (inventory.getFileId(objectRoot.path().relativize(file)) == null) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}. It should be manually deleted.", file, e);
                }
            }
        });
    }

    private void verifyPriorInventoryMutable(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, boolean isNewMutableHead) {
        Path sidecarPath;

        if (isNewMutableHead) {
            sidecarPath = objectRoot.inventorySidecar();
        } else {
            sidecarPath = objectRoot.mutableHeadVersion().inventorySidecar();
        }

        verifyPriorInventory(inventory, sidecarPath);
    }

    private void verifyPriorInventory(Inventory inventory, Path sidecarPath) {
        if (inventory.getPreviousDigest() != null) {
            var actualDigest = SidecarMapper.readDigestRequired(sidecarPath);
            if (!actualDigest.equalsIgnoreCase(inventory.getPreviousDigest())) {
                throw new ObjectOutOfSyncException(String.format("Cannot update object %s because the update is out of sync with the current object state. " +
                                "The digest of the current inventory is %s, but the digest %s was expected.",
                        inventory.getId(), actualDigest, inventory.getPreviousDigest()));
            }
        } else if (!inventory.getHead().equals(VersionNum.V1)) {
            LOG.debug("Cannot verify prior inventory for object {} because its digest is unknown.", inventory.getId());
        }
    }

    private Stream<Path> findOcflObjectRootDirs(Path start) {
        var iterator = new FileSystemOcflObjectRootDirIterator(start);
        try {
            var spliterator = Spliterators.spliteratorUnknownSize(iterator,
                    Spliterator.ORDERED | Spliterator.IMMUTABLE | Spliterator.DISTINCT);
            return StreamSupport.stream(spliterator, false)
                    .map(Paths::get)
                    .onClose(iterator::close);
        } catch (RuntimeException e) {
            iterator.close();
            throw e;
        }
    }

    private void ensureNoMutableHead(ObjectPaths.ObjectRoot objectRoot) {
        if (Files.exists(objectRoot.mutableHeadVersion().inventoryFile())) {
            throw new OcflStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    objectRoot.objectId()));
        }
    }

    private void ensureRootObjectHasNotChanged(Inventory inventory, ObjectPaths.ObjectRoot objectRoot) {
        var savedSidecarPath = ObjectPaths.inventorySidecarPath(objectRoot.mutableHeadExtensionPath(), inventory);

        String expectedDigest;
        try {
            expectedDigest = SidecarMapper.readDigestOptional(savedSidecarPath);
        } catch (OcflNoSuchFileException e) {
            return;
        }

        var actualDigest = SidecarMapper.readDigestRequired(objectRoot.inventorySidecar());

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new ObjectOutOfSyncException(
                    String.format("The mutable HEAD of object %s is out of sync with the root object state.", inventory.getId()));
        }
    }

    private void ensureRootObjectHasNotChanged(String objectId, Path objectRootPath) {
        var savedSidecarPath = ObjectPaths.findMutableHeadRootInventorySidecarPath(objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH));
        String expectedDigest;
        try {
            expectedDigest = SidecarMapper.readDigestOptional(savedSidecarPath);
        } catch (OcflNoSuchFileException e) {
            return;
        }

        var rootSidecarPath = ObjectPaths.findInventorySidecarPath(objectRootPath);
        var actualDigest = SidecarMapper.readDigestRequired(rootSidecarPath);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new ObjectOutOfSyncException(
                    String.format("The mutable HEAD of object %s is out of sync with the root object state.", objectId));
        }
    }

    private ObjectProperties examineObject(Path objectRootPath) {
        var properties = new ObjectProperties();

        try (var paths = Files.list(objectRootPath)) {
            var allPaths = paths.collect(Collectors.toList());

            for (var path : allPaths) {
                var filename = path.getFileName().toString();

                if (Files.isRegularFile(path)) {
                    if (filename.startsWith(OcflConstants.INVENTORY_SIDECAR_PREFIX)) {
                        properties.setDigestAlgorithm(SidecarMapper.getDigestAlgorithmFromSidecar(path));
                    } else if (filename.startsWith(OcflConstants.OBJECT_NAMASTE_PREFIX)) {
                        properties.setOcflVersion(OcflVersion.fromOcflObjectVersionFilename(filename));
                    }
                } else if (Files.isDirectory(path)) {
                    if (OcflConstants.EXTENSIONS_DIR.equals(filename)) {
                        properties.setExtensions(true);
                    }
                }

                if (properties.getOcflVersion() != null
                        && properties.getDigestAlgorithm() != null
                        && properties.hasExtensions()) {
                    break;
                }
            }
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            throw new OcflIOException(e);
        }

        return properties;
    }

    private Set<String> loadObjectExtensions(Path objectRoot) {
        var extensions = new HashSet<String>();
        // Currently, this just ensures that the object does not use any extensions that ocfl-java does not support
        var extensionsDir = ObjectPaths.extensionsPath(objectRoot);
        try (var list = Files.list(extensionsDir)) {
            list.filter(Files::isDirectory).forEach(dir -> {
                var extensionName = dir.getFileName().toString();
                if (supportEvaluator.checkSupport(extensionName)) {
                    extensions.add(extensionName);
                }
            });
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            throw new OcflIOException(e);
        }

        return extensions;
    }

}
