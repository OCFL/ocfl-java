package edu.wisc.library.ocfl.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.DigestAlgorithmRegistry;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.*;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class FileSystemOcflStorage implements OcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorage.class);

    private Path repositoryRoot;
    private ObjectIdPathMapper objectIdPathMapper;
    private InventoryMapper inventoryMapper;

    private ParallelProcess parallelProcess;

    /**
     * Creates a new FileSystemOcflStorage object. Its thread pool is size is set to the number of available processors.
     *
     * @param repositoryRoot OCFL repository root directory
     * @param objectIdPathMapper Mapper for mapping object ids to paths within the repository root
     */
    public FileSystemOcflStorage(Path repositoryRoot, ObjectIdPathMapper objectIdPathMapper) {
        this(repositoryRoot, objectIdPathMapper, Runtime.getRuntime().availableProcessors());
    }

    /**
     * Creates a new FileSystemOcflStorage object.
     *
     * @param repositoryRoot OCFL repository root directory
     * @param objectIdPathMapper Mapper for mapping object ids to paths within the repository root
     * @param threadPoolSize The size of the object's thread pool, used when calculating digests
     */
    public FileSystemOcflStorage(Path repositoryRoot, ObjectIdPathMapper objectIdPathMapper, int threadPoolSize) {
        this.repositoryRoot = Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        this.objectIdPathMapper = Enforce.notNull(objectIdPathMapper, "objectIdPathMapper cannot be null");
        Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");

        this.inventoryMapper = InventoryMapper.defaultMapper(); // This class will never serialize an Inventory, so the pretty print doesn't matter
        this.parallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(threadPoolSize)));
    }

    public FileSystemOcflStorage setInventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        Inventory inventory = null;
        var objectRootPath = objectRootPathFull(objectId);

        if (Files.exists(objectRootPath)) {
            var mutableHeadInventoryPath = ObjectPaths.mutableHeadInventoryPath(objectRootPath);
            if (Files.exists(mutableHeadInventoryPath)) {
                ensureRootObjectHasNotChanged(objectId, objectRootPath);
                inventory = parseMutableHeadInventory(mutableHeadInventoryPath);
            } else {
                inventory = parseInventory(ObjectPaths.inventoryPath(objectRootPath));
            }
        }

        return inventory;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        var objectRootPath = objectRootPathFull(inventory.getId());
        var objectRoot = ObjectPaths.objectRoot(inventory, objectRootPath);

        if (inventory.hasMutableHead()) {
            storeNewMutableHeadVersion(inventory, objectRoot, stagingDir);
        } else {
            storeNewVersion(inventory, objectRoot, stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = ensureVersion(inventory, versionId);
        var algorithm = inventory.getDigestAlgorithm();

        var map = new HashMap<String, OcflFileRetriever>(version.getState().size());

        version.getState().forEach((digest, paths) -> {
            var srcPath = objectRootPath.resolve(ensureManifestPath(inventory, digest));

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
        var objectRootPath = objectRootPathFull(inventory.getId());
        var version = ensureVersion(inventory, versionId);

        parallelProcess.collection(version.getState().entrySet(), entry -> {
            var id = entry.getKey();
            var files = entry.getValue();

            var src = ensureManifestPath(inventory, id);
            var srcPath = objectRootPath.resolve(src);

            for (var dstPath : files) {
                var path = stagingDir.resolve(dstPath);

                if (Thread.interrupted()) {
                    break;
                } else {
                    FileUtil.copyFileMakeParents(srcPath, path);
                }

                if (Thread.interrupted()) {
                    break;
                } else {
                    var digest = DigestUtil.computeDigest(inventory.getDigestAlgorithm(), path);
                    var paths = inventory.getFilePaths(digest);
                    if (paths == null || !paths.contains(src)) {
                        throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Was: %s",
                                path, inventory.getId(), inventory.getDigestAlgorithm().getOcflName(), digest));
                    }
                }
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public InputStream retrieveFile(Inventory inventory, String fileId) {
        var objectRootPath = objectRootPathFull(inventory.getId());

        var filePath = inventory.getFilePath(fileId);

        if (filePath == null) {
            throw new NotFoundException(String.format("File %s does not exist in object %s.", fileId, inventory.getId()));
        }

        try {
            return Files.newInputStream(objectRootPath.resolve(filePath));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
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
    public void commitMutableHead(Inventory inventory, Path stagingDir) {
        var objectRootPath = objectRootPathFull(inventory.getId());
        var objectRoot = ObjectPaths.objectRoot(inventory, objectRootPath);

        ensureRootObjectHasNotChanged(inventory, objectRoot);

        if (!Files.exists(objectRoot.mutableHeadVersion().inventoryFile())) {
            throw new ObjectOutOfSyncException(String.format("Cannot commit mutable HEAD of object %s because a mutable HEAD does not exist.", inventory.getId()));
        }

        var versionRoot = objectRoot.headVersion();

        createVersionDirectory(inventory, versionRoot);

        try {
            // TODO does moving this back in failure cases make sense?
            // TODO should a copy be done instead?
            FileUtil.moveDirectory(objectRoot.mutableHeadPath(), versionRoot.path());
            versionContentFixityCheck(inventory, versionRoot.contentPath(), inventory.getHead().toString(), objectRoot.path());

            // TODO make backup?
            copyInventory(ObjectPaths.version(inventory, stagingDir), versionRoot);
            FileUtil.deleteEmptyDirs(versionRoot.contentPath());

            copyInventoryToRootWithRollback(versionRoot, objectRoot, inventory);
            // TODO verify inventory integrity again?
            // TODO failure conditions of this?
            FileUtil.safeDeletePath(objectRoot.mutableHeadExtensionPath());
        } catch (RuntimeException e) {
            // TODO this results in the loss of everything that was in the mutable HEAD...
            FileUtil.safeDeletePath(versionRoot.path());
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeMutableHead(String objectId) {
        var objectRootPath = objectRootPathFull(objectId);
        var extensionRoot = objectRootPath.resolve(OcflConstants.MUTABLE_HEAD_EXT_PATH);

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
        return Files.exists(ObjectPaths.inventoryPath(objectRootPathFull(objectId)));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String objectRootPath(String objectId) {
        return objectIdPathMapper.map(objectId).toString();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeStorage(String ocflVersion) {
        if (!Files.exists(repositoryRoot)) {
            FileUtil.createDirectories(repositoryRoot);
        } else {
            Enforce.expressionTrue(Files.isDirectory(repositoryRoot), repositoryRoot,
                    "repositoryRoot must be a directory");
        }

        if (repositoryRoot.toFile().list().length == 0) {
            // setup new repo
            // TODO perhaps this should be moved somewhere else so it can be used by other storage implementations
            new NamasteTypeFile(ocflVersion).writeFile(repositoryRoot);
            writeOcflSpec(ocflVersion);
            writeOcflLayout();
        } else {
            validateExistingRepo(ocflVersion);
        }
    }

    private Path objectRootPathFull(String objectId) {
        return repositoryRoot.resolve(objectIdPathMapper.map(objectId));
    }

    private Inventory parseInventory(Path inventoryPath) {
        if (Files.notExists(inventoryPath)) {
            // TODO if there's not root inventory should we look for the inventory in the latest version directory?
            throw new IllegalStateException("Missing inventory at " + inventoryPath);
        }
        verifyInventory(inventoryPath);
        return inventoryMapper.read(inventoryPath);
    }

    private Inventory parseMutableHeadInventory(Path inventoryPath) {
        verifyInventory(inventoryPath);
        var revisionId = identifyLatestRevision(inventoryPath.getParent());
        return inventoryMapper.readMutableHead(revisionId, inventoryPath);
    }

    private void verifyInventory(Path inventoryPath) {
        var sidecarPath = findInventorySidecar(inventoryPath.getParent());
        var expectedDigest = readInventoryDigest(sidecarPath);
        var algorithm = getDigestAlgorithmFromSidecar(sidecarPath);

        var actualDigest = DigestUtil.computeDigest(algorithm, inventoryPath);

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
                    .map(RevisionId::fromValue)
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
                throw new IllegalStateException(String.format("Expected there to be one inventory sidecar file in %s, but found %s.",
                        path, sidecars.size()));
            }

            return sidecars.get(0);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private String readInventoryDigest(Path inventorySidecarPath) {
        try {
            var parts = Files.readString(inventorySidecarPath).split("\\s");
            if (parts.length == 0) {
                throw new IllegalStateException("Invalid inventory sidecar file: " + inventorySidecarPath);
            }
            return parts[0];
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    // TODO enforce sha256/sha512?
    private DigestAlgorithm getDigestAlgorithmFromSidecar(Path inventorySidecarPath) {
        return DigestAlgorithmRegistry.getAlgorithm(
                inventorySidecarPath.getFileName().toString().substring(OcflConstants.INVENTORY_FILE.length() + 1));
    }

    private void storeNewVersion(Inventory inventory, ObjectPaths.ObjectRoot objectRoot, Path stagingDir) {
        ensureNoMutableHead(objectRoot);

        var versionRoot = objectRoot.headVersion();

        var isFirstVersion = isFirstVersion(inventory);

        try {
            if (isFirstVersion) {
                setupNewObjectDirs(objectRoot.path());
            }

            createVersionDirectory(inventory, versionRoot);

            try {
                FileUtil.moveDirectory(stagingDir, versionRoot.path());
                versionContentFixityCheck(inventory, versionRoot.contentPath(), inventory.getHead().toString(), objectRoot.path());
                copyInventoryToRootWithRollback(versionRoot, objectRoot, inventory);
                // TODO verify inventory integrity again?
            } catch (RuntimeException e) {
                FileUtil.safeDeletePath(versionRoot.path());
                throw e;
            }
        } catch (RuntimeException e) {
            if (isFirstVersion) {
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
            createRevisionDirectory(inventory, versionRoot);

            // TODO there's a little funniness in the ordering here...
            if (isNewMutableHead) {
                var rootSidecar = objectRoot.inventorySidecar();
                FileUtil.copy(rootSidecar,
                        versionRoot.path().getParent().resolve("root-" + rootSidecar.getFileName().toString()),
                        StandardCopyOption.REPLACE_EXISTING);
            }

            try {
                FileUtil.moveDirectory(stagingVersionRoot.contentRoot().headRevisionPath(), revisionPath);
                versionContentFixityCheck(inventory, revisionPath, objectRoot.path().relativize(revisionPath).toString(), objectRoot.path());

                // TODO handle failure? backup?
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

        deleteMutableHeadFilesNotInManifest(inventory, objectRoot, versionRoot);
    }

    private void createVersionDirectory(Inventory inventory, ObjectPaths.VersionRoot versionRoot) {
        try {
            Files.createDirectory(versionRoot.path());
        } catch (FileAlreadyExistsException e) {
            throw new ObjectOutOfSyncException(
                    String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void createRevisionDirectory(Inventory inventory, ObjectPaths.VersionRoot versionRoot) {
        try {
            Files.createDirectories(versionRoot.contentPath());
            Files.createDirectory(versionRoot.contentRoot().headRevisionPath());
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
        FileUtil.createDirectories(objectRootPath);
        new NamasteTypeFile(OcflConstants.OCFL_OBJECT_VERSION).writeFile(objectRootPath);
    }

    private void copyInventory(ObjectPaths.HasInventory source, ObjectPaths.HasInventory destination) {
        FileUtil.copy(source.inventoryFile(), destination.inventoryFile(), StandardCopyOption.REPLACE_EXISTING);
        FileUtil.copy(source.inventorySidecar(), destination.inventorySidecar(), StandardCopyOption.REPLACE_EXISTING);
    }

    private void copyInventoryToRootWithRollback(ObjectPaths.HasInventory source, ObjectPaths.ObjectRoot objectRoot, Inventory inventory) {
        try {
            copyInventory(source, objectRoot);
        } catch (RuntimeException e) {
            try {
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
            if (inventory.getFileId(objectRoot.path().relativize(file).toString()) == null) {
                try {
                    Files.delete(file);
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", file, e);
                }
            }
        });
    }

    private void versionContentFixityCheck(Inventory inventory, Path fileRoot, String contentPrefix, Path objectRootPath) {
        var version = inventory.getHeadVersion();
        var files = FileUtil.findFiles(fileRoot);
        var fileIds = inventory.getFileIdsForMatchingFiles(contentPrefix + "/");

        var expected = ConcurrentHashMap.<String>newKeySet(fileIds.size());
        expected.addAll(fileIds);

        parallelProcess.collection(files, file -> {
            var fileContentPath = objectRootPath.relativize(file);
            var expectedDigest = inventory.getFileId(fileContentPath.toString());
            if (expectedDigest == null) {
                throw new IllegalStateException(String.format("File not listed in object %s manifest: %s",
                        inventory.getId(), fileContentPath));
            } else if (version.getPaths(expectedDigest) == null) {
                throw new IllegalStateException(String.format("File not found in object %s version %s state: %s",
                        inventory.getId(), inventory.getHead(), fileContentPath));
            } else {
                var actualDigest = DigestUtil.computeDigest(inventory.getDigestAlgorithm(), file);
                if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
                    throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Expected: %s; Actual: %s",
                            file, inventory.getId(), inventory.getDigestAlgorithm().getOcflName(), expectedDigest, actualDigest));
                }

                expected.remove(expectedDigest);
            }
        });

        if (!expected.isEmpty()) {
            var filePaths = expected.stream().map(inventory::getFilePath).collect(Collectors.toList());
            throw new IllegalStateException(String.format("Object %s is missing the following files: %s", inventory.getId(), filePaths));
        }
    }

    private Version ensureVersion(Inventory inventory, VersionId versionId) {
        var version = inventory.getVersion(versionId);

        if (version == null) {
            throw new IllegalStateException(String.format("Object %s does not contain version %s", inventory.getId(), versionId));
        }

        return version;
    }

    private String ensureManifestPath(Inventory inventory, String id) {
        if (!inventory.manifestContainsId(id)) {
            throw new IllegalStateException(String.format("Missing manifest entry for %s in object %s.",
                    id, inventory.getId()));
        }
        return inventory.getFilePath(id);
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
            var expectedDigest = readInventoryDigest(savedSidecarPath);
            var actualDigest = readInventoryDigest(objectRoot.inventorySidecar());

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
            var expectedDigest = readInventoryDigest(savedSidecarPath);
            var actualDigest = readInventoryDigest(rootSidecarPath);

            if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
                throw new ObjectOutOfSyncException(
                        String.format("The mutable HEAD of object %s is out of sync with the root object state.", objectId));
            }
        }
    }

    private void validateExistingRepo(String ocflVersion) {
        String existingOcflVersion = null;

        for (var file : repositoryRoot.toFile().listFiles()) {
            if (file.isFile() && file.getName().startsWith("0=")) {
                existingOcflVersion = file.getName().substring(2);
                break;
            }
        }

        if (existingOcflVersion == null) {
            throw new IllegalStateException("OCFL root is missing its root conformance declaration.");
        } else if (!existingOcflVersion.equals(ocflVersion)) {
            throw new IllegalStateException(String.format("OCFL version mismatch. Expected: %s; Found: %s",
                    ocflVersion, existingOcflVersion));
        }

        var objectRoot = identifyRandomObjectRoot(repositoryRoot);

        if (objectRoot != null) {
            var inventory = parseInventory(ObjectPaths.inventoryPath(objectRoot));
            var expectedPath = objectIdPathMapper.map(inventory.getId());
            var actualPath = repositoryRoot.relativize(objectRoot);
            if (!expectedPath.equals(actualPath)) {
                throw new IllegalStateException(String.format(
                        "The OCFL client was configured to use the following layout: %s." +
                                " This layout does not match the layout of existing objects in the repository." +
                        " Found object %s stored at %s, but was expecting it to be stored at %s.",
                        objectIdPathMapper.describeLayout(), inventory.getId(), actualPath, expectedPath
                ));
            }
        }
    }

    private void writeOcflSpec(String ocflVersion) {
        var ocflSpecFile = ocflVersion + ".txt";
        try (var ocflSpecStream = FileSystemOcflStorage.class.getClassLoader().getResourceAsStream(ocflSpecFile)) {
            Files.copy(ocflSpecStream, repositoryRoot.resolve(ocflSpecFile));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private Path identifyRandomObjectRoot(Path root) {
        var objectRootHolder = new ArrayList<Path>(1);
        var objectMarkerPrefix = "0=ocfl_object";

        try {
            Files.walkFileTree(root, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (dir.endsWith("deposit")) {
                        return FileVisitResult.SKIP_SUBTREE;
                    }
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    if (file.getFileName().toString().startsWith(objectMarkerPrefix)) {
                        objectRootHolder.add(file.getParent());
                        return FileVisitResult.TERMINATE;
                    }
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        if (objectRootHolder.isEmpty()) {
            return null;
        }

        return objectRootHolder.get(0);
    }

    private void writeOcflLayout() {
        try {
            var map = new TreeMap<String, Object>(Comparator.naturalOrder());
            map.putAll(objectIdPathMapper.describeLayout());
            new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true)
                    .writeValue(repositoryRoot.resolve("ocfl_layout.json").toFile(), map);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
