package edu.wisc.library.ocfl.core.storage.filesystem;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraints;
import edu.wisc.library.ocfl.core.storage.AbstractOcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.*;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class FileSystemOcflStorage extends AbstractOcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorage.class);

    private PathConstraintProcessor logicalPathConstraints;

    private Path repositoryRoot;
    private SidecarMapper sidecarMapper;
    private FileSystemOcflStorageInitializer initializer;
    private ParallelProcess parallelProcess;

    private boolean checkNewVersionFixity;

    private ObjectIdPathMapper objectIdPathMapper;

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

        this.logicalPathConstraints = PathConstraints.logicalPathConstraints();
        this.sidecarMapper = new SidecarMapper();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        Inventory inventory = null;
        var objectRootPathStr = objectRootPath(objectId);
        var objectRootPath = repositoryRoot.resolve(objectRootPathStr);

        if (Files.exists(objectRootPath)) {
            var mutableHeadInventoryPath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);
            if (Files.exists(mutableHeadInventoryPath)) {
                ensureRootObjectHasNotChanged(objectId, objectRootPath);
                inventory = parseMutableHeadInventory(objectRootPathStr, mutableHeadInventoryPath);
            } else {
                inventory = parseInventory(objectRootPathStr, ObjectPaths.inventoryPath(objectRootPath));
            }
        }

        return inventory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

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

                SafeFiles.createDirectories(destination.getParent());

                if (Thread.interrupted()) {
                    break;
                }

                try (var stream = new FixityCheckInputStream(Files.newInputStream(srcPath), digestAlgorithm, id)) {
                    Files.copy(stream, destination);
                    stream.checkFixity();
                } catch (IOException e) {
                    throw new RuntimeIOException(e);
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

        var objectRootPath = objectRootPathFull(objectId);

        if (Files.exists(objectRootPath)) {
            try (var paths = Files.walk(objectRootPath)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(f -> {
                            try {
                                Files.delete(f);
                            } catch (IOException e) {
                                throw new RuntimeIOException(String.format("Failed to delete file %s while purging object %s." +
                                        " The purge failed the object may need to be deleted manually.", f, objectId), e);
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeIOException(String.format("Failed to purge object %s at %s. The object may need to be deleted manually.",
                        objectId, objectRootPath), e);
            }
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir) {
        ensureOpen();

        var objectRootPath = objectRootPathFull(newInventory.getId());
        var objectRoot = ObjectPaths.objectRoot(newInventory, objectRootPath);

        ensureRootObjectHasNotChanged(newInventory, objectRoot);

        if (!Files.exists(objectRoot.mutableHeadVersion().inventoryFile())) {
            throw new ObjectOutOfSyncException(
                    String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", newInventory.getId()));
        }

        var versionRoot = objectRoot.headVersion();
        var stagingRoot = ObjectPaths.version(newInventory, stagingDir);

        versionContentCheck(oldInventory, objectRoot, objectRoot.mutableHeadVersion().contentPath(), true);

        moveToVersionDirectory(newInventory, objectRoot.mutableHeadPath(), versionRoot);

        try {
            try {
                // The inventory is written to the root first so that the mutable version can be recovered if the write fails
                copyInventoryToRootWithRollback(stagingRoot, objectRoot, newInventory);
                // TODO this is still slightly dangerous if one file succeeds and the other fails...
                copyInventory(stagingRoot, versionRoot);
            } catch (RuntimeException e) {
                try {
                    FileUtil.moveDirectory(versionRoot.path(), objectRoot.mutableHeadPath());
                } catch (RuntimeException | FileAlreadyExistsException e1) {
                    LOG.error("Failed to move {} back to {}", versionRoot.path(), objectRoot.mutableHeadPath(), e1);
                }
                throw e;
            }

            try {
                // TODO need to decide how to handle empty revisions..
                FileUtil.deleteEmptyDirs(versionRoot.contentPath());
            } catch (RuntimeException e) {
                // This does not fail the commit
                LOG.error("Failed to delete an empty directory. It may need to be deleted manually.", e);
            }
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(versionRoot.path());
            throw e;
        }

        // TODO failure conditions of this?
        FileUtil.safeDeletePath(objectRoot.mutableHeadExtensionPath());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        var objectRootPath = objectRootPathFull(objectId);
        var extensionRoot = ObjectPaths.mutableHeadExtensionRoot(objectRootPath);

        if (Files.exists(extensionRoot)) {
            try (var paths = Files.walk(extensionRoot)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(f -> {
                            try {
                                Files.delete(f);
                            } catch (IOException e) {
                                throw new RuntimeIOException(String.format("Failed to delete file %s while purging mutable HEAD of object %s." +
                                        " The purge failed and the mutable HEAD may need to be deleted manually.", f, objectId), e);
                            }
                        });
            } catch (IOException e) {
                throw new RuntimeIOException(String.format("Failed to purge mutable HEAD of object %s at %s. The object may need to be deleted manually.",
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

        return Files.exists(ObjectPaths.inventoryPath(objectRootPathFull(objectId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String objectRootPath(String objectId) {
        ensureOpen();

        return objectIdPathMapper.map(objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void doInitialize(LayoutConfig layoutConfig) {
        this.objectIdPathMapper = this.initializer.initializeStorage(repositoryRoot, ocflVersion, layoutConfig);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        parallelProcess.shutdown();
    }

    private Path objectRootPathFull(String objectId) {
        return repositoryRoot.resolve(objectIdPathMapper.map(objectId));
    }

    private Inventory parseInventory(String objectRootPath, Path inventoryPath) {
        if (Files.notExists(inventoryPath)) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new CorruptObjectException("Missing inventory at " + inventoryPath);
        }
        verifyInventory(inventoryPath);
        return inventoryMapper.read(objectRootPath, inventoryPath);
    }

    private Inventory parseMutableHeadInventory(String objectRootPath, Path inventoryPath) {
        verifyInventory(inventoryPath);
        var revisionId = identifyLatestRevision(inventoryPath.getParent());
        return inventoryMapper.readMutableHead(objectRootPath, revisionId, inventoryPath);
    }

    private void verifyInventory(Path inventoryPath) {
        var sidecarPath = findInventorySidecar(inventoryPath.getParent());
        var expectedDigest = sidecarMapper.readSidecar(sidecarPath);
        var algorithm = sidecarMapper.getDigestAlgorithmFromSidecar(FileUtil.pathToStringStandardSeparator(sidecarPath));

        var actualDigest = DigestUtil.computeDigestHex(algorithm, inventoryPath);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Invalid inventory file: %s. Expected %s digest: %s; Actual: %s",
                    inventoryPath, algorithm.getOcflName(), expectedDigest, actualDigest));
        }
    }

    private RevisionId identifyLatestRevision(Path versionPath) {
        try (var files = Files.list(versionPath)) {
            var result = files.filter(Files::isDirectory)
                    .map(Path::getFileName).map(Path::toString)
                    .filter(RevisionId::isRevisionId)
                    .map(RevisionId::fromString)
                    .max(Comparator.naturalOrder());
            if (result.isEmpty()) {
                return null;
            }
            return result.get();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
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
            throw new RuntimeIOException(e);
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
                // TODO verify inventory integrity again?
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

        var isNewMutableHead = Files.notExists(versionRoot.inventoryFile());

        try {
            moveToRevisionDirectory(inventory, stagingVersionRoot, versionRoot);

            if (isNewMutableHead) {
                copyRootInventorySidecar(objectRoot, versionRoot);
            }

            try {
                versionContentCheck(inventory, objectRoot, revisionPath, checkNewVersionFixity);
                copyInventory(stagingVersionRoot, versionRoot);
                // TODO verify inventory integrity?
            } catch (RuntimeException e) {
                FileUtil.safeDeletePath(revisionPath);
                throw e;
            }
        } catch (RuntimeException e) {
            if (isNewMutableHead) {
                FileUtil.safeDeletePath(versionRoot.path().getParent());
            }
            throw e;
        }

        // TODO since this isn't guaranteed to have completed do we need to run it on commit?
        deleteMutableHeadFilesNotInManifest(inventory, objectRoot, versionRoot);
    }

    private void copyRootInventorySidecar(ObjectPaths.ObjectRoot objectRoot, ObjectPaths.VersionRoot versionRoot) {
        var rootSidecar = objectRoot.inventorySidecar();
        SafeFiles.copy(rootSidecar,
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
            throw new RuntimeIOException(e);
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
            throw new RuntimeIOException(e);
        }
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private void setupNewObjectDirs(Path objectRootPath) {
        SafeFiles.createDirectories(objectRootPath);
        new NamasteTypeFile(ocflVersion.getOcflObjectVersion()).writeFile(objectRootPath);
    }

    private void copyInventory(ObjectPaths.HasInventory source, ObjectPaths.HasInventory destination) {
        SafeFiles.copy(source.inventoryFile(), destination.inventoryFile(), StandardCopyOption.REPLACE_EXISTING);
        SafeFiles.copy(source.inventorySidecar(), destination.inventorySidecar(), StandardCopyOption.REPLACE_EXISTING);
    }

    private void copyInventoryToRootWithRollback(ObjectPaths.HasInventory source, ObjectPaths.ObjectRoot objectRoot, Inventory inventory) {
        try {
            copyInventory(source, objectRoot);
        } catch (RuntimeException e) {
            try {
                // TODO bug here when first version
                var previousVersionRoot = objectRoot.version(inventory.getHead().previousVersionId());
                copyInventory(previousVersionRoot, objectRoot);
            } catch (RuntimeException e1) {
                LOG.error("Failed to rollback inventory at {}", objectRoot.inventoryFile(), e1);
            }
            throw e;
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
