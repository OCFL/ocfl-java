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

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.LogicalPathConstraints;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.storage.AbstractOcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import net.jodah.failsafe.Failsafe;
import net.jodah.failsafe.RetryPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

public class FileSystemOcflStorage extends AbstractOcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorage.class);

    private final PathConstraintProcessor logicalPathConstraints;

    private final Path repositoryRoot;
    private final SidecarMapper sidecarMapper;
    private final FileSystemOcflStorageInitializer initializer;
    private final ParallelProcess parallelProcess;

    private final boolean checkNewVersionFixity;

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
     * @param threadPoolSize The size of the object's thread pool, used when calculating digests
     * @param checkNewVersionFixity If a fixity check should be performed on the contents of a new version's
     *                              content directory after moving it into the object. In most cases, this should not be
     *                              required, especially if the OCFL client's work directory is on the same volume as the
     *                              storage root.
     * @param initializer initializes a new OCFL repo
     */
    public FileSystemOcflStorage(Path repositoryRoot, int threadPoolSize, boolean checkNewVersionFixity,
                                 FileSystemOcflStorageInitializer initializer) {
        this.repositoryRoot = Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");
        this.parallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(threadPoolSize)));
        this.checkNewVersionFixity = checkNewVersionFixity;
        this.initializer = Enforce.notNull(initializer, "initializer cannot be null");

        this.logicalPathConstraints = LogicalPathConstraints.constraintsWithBackslashCheck();
        this.sidecarMapper = new SidecarMapper();
        this.ioRetry = new RetryPolicy<Void>()
                .handle(UncheckedIOException.class, IOException.class)
                .withBackoff(50, 200, ChronoUnit.MILLIS, 1.5)
                .withJitter(Duration.ofMillis(10))
                .withMaxRetries(5);
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

        if (Files.exists(objectRootPathAbsolute)) {
            var mutableHeadInventoryPath = ObjectPaths.mutableHeadInventoryPath(objectRootPathAbsolute);
            if (Files.exists(mutableHeadInventoryPath)) {
                ensureRootObjectHasNotChanged(objectId, objectRootPathAbsolute);
                inventory = parseMutableHeadInventory(objectRootPathStr, objectRootPathAbsolute, mutableHeadInventoryPath);
            } else {
                inventory = parseInventory(objectRootPathStr, ObjectPaths.inventoryPath(objectRootPathAbsolute));
            }
        }

        return inventory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<String> listObjectIds() {
        LOG.debug("List object ids");

        return findOcflObjectRootDirs(repositoryRoot).map(rootPath -> {
            var relativeRootStr = FileUtil.pathToStringStandardSeparator(repositoryRoot.relativize(rootPath));
            var inventoryPath = ObjectPaths.inventoryPath(rootPath);
            var inventory = inventoryMapper.read(relativeRootStr, inventoryPath);
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
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        ensureOpen();

        LOG.debug("Get file streams for object <{}> version <{}>", inventory.getId(), versionId);

        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = inventory.ensureVersion(versionId);
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
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {
        ensureOpen();

        LOG.debug("Reconstruct object <{}> version <{}> in directory <{}>", inventory.getId(), versionId, stagingDir);

        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = inventory.ensureVersion(versionId);
        var digestAlgorithm = inventory.getDigestAlgorithm().getJavaStandardName();

        parallelProcess.collection(version.getState().entrySet(), entry -> {
            var id = entry.getKey();
            var files = entry.getValue();

            var srcPath = objectRootPath.resolve(inventory.ensureContentPath(id));

            for (var logicalPath : files) {
                logicalPathConstraints.apply(logicalPath);
                var destination = Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), logicalPath));

                UncheckedFiles.createDirectories(destination.getParent());

                if (Thread.interrupted()) {
                    break;
                }

                try (var stream = new FixityCheckInputStream(Files.newInputStream(srcPath), digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
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

        if (Files.exists(objectRootPath)) {
            try (var paths = Files.walk(objectRootPath)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(f -> {
                            try {
                                Files.delete(f);
                            } catch (IOException e) {
                                throw new UncheckedIOException(String.format("Failed to delete file %s while purging object %s." +
                                        " The purge failed the object may need to be deleted manually.", f, objectId), e);
                            }
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(String.format("Failed to purge object %s at %s. The object may need to be deleted manually.",
                        objectId, objectRootPath), e);
            }
        }

        try {
            FileUtil.deleteDirAndParentsIfEmpty(objectRootPath.getParent());
        } catch (UncheckedIOException e) {
            LOG.error(String.format("Failed to cleanup all empty directories in path %s." +
                    " There may be empty directories remaining in the OCFL storage hierarchy.", objectRootPath));
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
            versionContentCheck(newInventory, objectRoot, versionRoot.contentPath(), checkNewVersionFixity);

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
            FileUtil.safeDeletePath(versionRoot.path());
            throw e;
        }

        FileUtil.safeDeletePath(objectRoot.mutableHeadExtensionPath());
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
                                throw new UncheckedIOException(String.format("Failed to delete file %s while purging mutable HEAD of object %s." +
                                        " The purge failed and the mutable HEAD may need to be deleted manually.", f, objectId), e);
                            }
                        });
            } catch (IOException e) {
                throw new UncheckedIOException(String.format("Failed to purge mutable HEAD of object %s at %s. The object may need to be deleted manually.",
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

        var exists = Files.exists(ObjectPaths.inventoryPath(objectRootPathFull(objectId)));

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
    protected void doInitialize(OcflExtensionConfig layoutConfig) {
        this.storageLayoutExtension = this.initializer.initializeStorage(repositoryRoot, ocflVersion, layoutConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Closing " + this.getClass().getName());
        parallelProcess.shutdown();
    }

    private Path objectRootPathFull(String objectId) {
        return repositoryRoot.resolve(storageLayoutExtension.mapObjectId(objectId));
    }

    private Inventory parseInventory(String objectRootPath, Path inventoryPath) {
        if (Files.notExists(inventoryPath)) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException("Missing inventory at " + inventoryPath);
        }

        try (var inventoryStream = inventoryVerifyingInputStream(inventoryPath)) {
            var inventory = inventoryMapper.read(objectRootPath, inventoryStream);
            inventoryStream.checkFixity();
            return inventory;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Inventory parseMutableHeadInventory(String objectRootPath, Path objectRootPathAbsolute, Path inventoryPath) {
        var revisionId = identifyLatestRevision(objectRootPathAbsolute);

        try (var inventoryStream = inventoryVerifyingInputStream(inventoryPath)) {
            var inventory = inventoryMapper.readMutableHead(objectRootPath, revisionId, inventoryStream);
            inventoryStream.checkFixity();
            return inventory;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private FixityCheckInputStream inventoryVerifyingInputStream(Path inventoryPath) throws IOException {
        var sidecarPath = findInventorySidecar(inventoryPath.getParent());
        var expectedDigest = sidecarMapper.readSidecar(sidecarPath);
        var algorithm = sidecarMapper.getDigestAlgorithmFromSidecar(FileUtil.pathToStringStandardSeparator(sidecarPath));

        return new FixityCheckInputStream(Files.newInputStream(inventoryPath), algorithm, expectedDigest);
    }

    private RevisionId identifyLatestRevision(Path objectRootPath) {
        var revisionsPath = ObjectPaths.mutableHeadRevisionsPath(objectRootPath);
        try (var files = Files.list(revisionsPath)) {
            var result = files.filter(Files::isRegularFile)
                    .map(Path::getFileName).map(Path::toString)
                    .filter(RevisionId::isRevisionId)
                    .map(RevisionId::fromString)
                    .max(Comparator.naturalOrder());
            if (result.isEmpty()) {
                return null;
            }
            return result.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path findInventorySidecar(Path objectRootPath) {
        return findGenericInventorySidecar(objectRootPath, OcflConstants.INVENTORY_FILE + ".");
    }

    private Path findGenericInventorySidecar(Path path, String prefix) {
        try (var files = Files.list(path)) {
            var sidecars = files
                    .filter(file -> file.getFileName().toString().startsWith(prefix))
                    .collect(Collectors.toList());

            if (sidecars.size() != 1) {
                throw new CorruptObjectException(String.format("Expected there to be one inventory sidecar file in %s, but found %s.",
                        path, sidecars.size()));
            }

            return sidecars.get(0);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
                versionContentCheck(inventory, objectRoot, versionRoot.contentPath(), checkNewVersionFixity);
                copyInventoryToRootWithRollback(versionRoot, objectRoot, inventory);
            } catch (RuntimeException e) {
                FileUtil.safeDeletePath(versionRoot.path());
                throw e;
            }
        } catch (RuntimeException e) {
            if (isFirstVersion && Files.notExists(objectRoot.inventoryFile())) {
                FileUtil.safeDeletePath(objectRoot.path());
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
                    versionContentCheck(inventory, objectRoot, revisionPath, checkNewVersionFixity);
                    copyInventory(stagingVersionRoot, versionRoot);
                } catch (RuntimeException e) {
                    FileUtil.safeDeletePath(revisionPath);
                    throw e;
                }
            } catch (RuntimeException e) {
                FileUtil.safeDeletePath(revisionMarker);
                throw e;
            }
        } catch (RuntimeException e) {
            if (isNewMutableHead) {
                FileUtil.safeDeletePath(versionRoot.path().getParent());
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
            throw new UncheckedIOException(e);
        }
    }

    private Path createRevisionMarker(Inventory inventory, Path revisionsPath) {
        UncheckedFiles.createDirectories(revisionsPath);
        var revision = inventory.getRevisionId().toString();
        try {
            var revisionMarker = Files.createFile(revisionsPath.resolve(revision));
            return Files.writeString(revisionMarker, revision);
        } catch (FileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to update mutable HEAD of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
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
            throw new UncheckedIOException(e);
        }
    }

    private boolean isFirstVersion(Inventory inventory) {
        return VersionId.V1.equals(inventory.getHead());
    }

    private void setupNewObjectDirs(Path objectRootPath) {
        UncheckedFiles.createDirectories(objectRootPath);
        new NamasteTypeFile(ocflVersion.getOcflObjectVersion()).writeFile(objectRootPath);
    }

    private void copyInventory(ObjectPaths.HasInventory source, ObjectPaths.HasInventory destination) {
        Failsafe.with(ioRetry).run(() -> {
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
                    var previousVersionRoot = objectRoot.version(inventory.getHead().previousVersionId());
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
                    Files.delete(file);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}. It should be manually deleted.", file, e);
                }
            }
        });
    }

    private void versionContentCheck(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, Path contentPath, boolean checkFixity) {
        var version = inventory.getHeadVersion();
        var files = FileUtil.findFiles(contentPath);
        var fileIds = inventory.getFileIdsForMatchingFiles(objectRoot.path().relativize(contentPath));

        var expected = ConcurrentHashMap.<String>newKeySet(fileIds.size());
        expected.addAll(fileIds);

        parallelProcess.collection(files, file -> {
            var fileContentPath = FileUtil.pathToStringStandardSeparator(objectRoot.path().relativize(file));
            var expectedDigest = inventory.getFileId(fileContentPath);

            if (expectedDigest == null) {
                throw new CorruptObjectException(String.format("File not listed in object %s manifest: %s",
                        inventory.getId(), fileContentPath));
            } else if (version.getPaths(expectedDigest) == null) {
                throw new CorruptObjectException(String.format("File not found in object %s version %s state: %s",
                        inventory.getId(), inventory.getHead(), fileContentPath));
            } else if (checkFixity) {
                var actualDigest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), file);
                if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
                    throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Expected: %s; Actual: %s",
                            file, inventory.getId(), inventory.getDigestAlgorithm().getOcflName(), expectedDigest, actualDigest));
                }
            }

            expected.remove(expectedDigest);
        });

        if (!expected.isEmpty()) {
            var filePaths = expected.stream().map(inventory::getContentPath).collect(Collectors.toList());
            throw new CorruptObjectException(String.format("Object %s is missing the following files: %s", inventory.getId(), filePaths));
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
            // TODO modeled exception?
            throw new IllegalStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    objectRoot.objectId()));
        }
    }

    private void ensureRootObjectHasNotChanged(Inventory inventory, ObjectPaths.ObjectRoot objectRoot) {
        var savedSidecarPath = ObjectPaths.inventorySidecarPath(objectRoot.mutableHeadExtensionPath(), inventory);
        if (Files.exists(savedSidecarPath)) {
            var expectedDigest = sidecarMapper.readSidecar(savedSidecarPath);
            var actualDigest = sidecarMapper.readSidecar(objectRoot.inventorySidecar());

            if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
                throw new ObjectOutOfSyncException(
                        String.format("The mutable HEAD of object %s is out of sync with the root object state.", inventory.getId()));
            }
        }
    }

    private void ensureRootObjectHasNotChanged(String objectId, Path objectRootPath) {
        var savedSidecarPath = findGenericInventorySidecar(objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH), "root-" + OcflConstants.INVENTORY_FILE + ".");
        if (Files.exists(savedSidecarPath)) {
            var rootSidecarPath = findInventorySidecar(objectRootPath);
            var expectedDigest = sidecarMapper.readSidecar(savedSidecarPath);
            var actualDigest = sidecarMapper.readSidecar(rootSidecarPath);

            if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
                throw new ObjectOutOfSyncException(
                        String.format("The mutable HEAD of object %s is out of sync with the root object state.", objectId));
            }
        }
    }

}
