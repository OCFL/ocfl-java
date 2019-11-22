package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.*;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.inventory.InventoryValidator;
import edu.wisc.library.ocfl.core.inventory.MutableHeadInventoryCommitter;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.path.ContentPathMapper;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Primary implementation of the OcflRepository API. It is storage agnostic. It is typically instantiated using
 * OcflRepositoryBuilder.
 *
 * @see OcflRepositoryBuilder
 */
public class DefaultOcflRepository implements MutableOcflRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflRepository.class);

    private OcflStorage storage;
    private InventoryMapper inventoryMapper;
    private Path workDir;
    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private ResponseMapper responseMapper;
    private InventoryUpdater.Builder inventoryUpdaterBuilder;
    private DefaultOcflObjectUpdater.Builder objectUpdaterBuilder;

    private OcflConfig config;

    private ParallelProcess parallelProcess;
    private ParallelProcess copyParallelProcess;

    private Clock clock;

    private boolean closed = false;

    /**
     * @see OcflRepositoryBuilder
     *
     * @param storage storage layer
     * @param workDir path to the directory to use for assembling ocfl versions
     * @param objectLock locking client
     * @param inventoryCache inventory cache
     * @param inventoryMapper object mapper for serializing inventories
     * @param pathSanitizer content path sanitizer
     * @param contentPathConstraintProcessor content path constraint processor
     * @param config ocfl defaults configuration
     * @param digestThreadPoolSize number of threads to use for computing digests
     * @param copyThreadPoolSize number of threads to use for copying files
     */
    public DefaultOcflRepository(OcflStorage storage, Path workDir,
                                 ObjectLock objectLock,
                                 Cache<String, Inventory> inventoryCache,
                                 InventoryMapper inventoryMapper,
                                 PathSanitizer pathSanitizer,
                                 ContentPathConstraintProcessor contentPathConstraintProcessor,
                                 OcflConfig config,
                                 int digestThreadPoolSize, int copyThreadPoolSize) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.config = Enforce.notNull(config, "config cannot be null");

        Enforce.expressionTrue(digestThreadPoolSize > 0, digestThreadPoolSize, "digestThreadPoolSize must be greater than 0");
        Enforce.expressionTrue(copyThreadPoolSize > 0, copyThreadPoolSize, "copyThreadPoolSize must be greater than 0");

        inventoryUpdaterBuilder = InventoryUpdater.builder().contentPathMapperBuilder(
                ContentPathMapper.builder()
                        .pathSanitizer(pathSanitizer)
                        .contentPathConstraintProcessor(contentPathConstraintProcessor));

        responseMapper = new ResponseMapper();
        clock = Clock.systemUTC();

        parallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(digestThreadPoolSize)));
        copyParallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(copyThreadPoolSize)));

        objectUpdaterBuilder = DefaultOcflObjectUpdater.builder()
                .parallelProcess(parallelProcess)
                .copyParallelProcess(copyParallelProcess);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId putObject(ObjectVersionId objectVersionId, Path path, CommitInfo commitInfo, OcflOption... ocflOptions) {
        ensureOpen();

        // TODO max object id length? should probably be 255 bytes

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        Enforce.notNull(path, "path cannot be null");

        var options = new HashSet<>(Arrays.asList(ocflOptions));

        var inventory = loadInventoryWithDefault(objectVersionId);
        ensureNoMutableHead(inventory);
        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var inventoryUpdater = inventoryUpdaterBuilder.buildBlankState(inventory);

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = createStagingContentDir(inventory, stagingDir);

        stageNewVersion(inventoryUpdater, path, contentDir, inventory.getDigestAlgorithm(), options);
        var newInventory = buildNewInventory(inventoryUpdater, commitInfo);

        try {
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId updateObject(ObjectVersionId objectVersionId, CommitInfo commitInfo, Consumer<OcflObjectUpdater> objectUpdater) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        Enforce.notNull(objectUpdater, "objectUpdater cannot be null");

        var inventory = loadInventoryWithDefault(objectVersionId);
        ensureNoMutableHead(inventory);
        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyState(inventory);

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = createStagingContentDir(inventory, stagingDir);

        try {
            objectUpdater.accept(objectUpdaterBuilder.build(inventory, inventoryUpdater, contentDir));
            var newInventory = buildNewInventory(inventoryUpdater, commitInfo);
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getObject(ObjectVersionId objectVersionId, Path outputPath) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        Enforce.notNull(outputPath, "outputPath cannot be null");
        Enforce.expressionTrue(Files.notExists(outputPath), outputPath, "outputPath must not exist");
        Enforce.expressionTrue(Files.exists(outputPath.getParent()), outputPath, "outputPath parent must exist");
        Enforce.expressionTrue(Files.isDirectory(outputPath.getParent()), outputPath, "outputPath parent must be a directory");

        var inventory = requireInventory(objectVersionId);
        requireVersion(objectVersionId, inventory);
        var versionId = resolveVersion(objectVersionId, inventory);

        getObjectInternal(inventory, versionId, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectVersion getObject(ObjectVersionId objectVersionId) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");

        var inventory = requireInventory(objectVersionId);
        requireVersion(objectVersionId, inventory);
        var versionId = resolveVersion(objectVersionId, inventory);

        var versionDetails = createVersionDetails(inventory, versionId);
        var objectStreams = storage.getObjectStreams(inventory, versionId);

        var files = versionDetails.getFiles().stream()
                .map(file -> {
                    return new OcflObjectVersionFile(file, objectStreams.get(file.getPath()));
                })
                .collect(Collectors.toMap(OcflObjectVersionFile::getPath, v -> v));

        versionDetails.setFileMap(null);

        return new OcflObjectVersion(versionDetails, files);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectDetails describeObject(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        var inventory = requireInventory(ObjectVersionId.head(objectId));

        return responseMapper.mapInventory(inventory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionDetails describeVersion(ObjectVersionId objectVersionId) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");

        var inventory = requireInventory(objectVersionId);
        requireVersion(objectVersionId, inventory);
        var versionId = resolveVersion(objectVersionId, inventory);

        return createVersionDetails(inventory, versionId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");
        return storage.containsObject(objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        objectLock.doInWriteLock(objectId, () -> {
            try {
                storage.purgeObject(objectId);
            } finally {
                inventoryCache.invalidate(objectId);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId stageChanges(ObjectVersionId objectVersionId, CommitInfo commitInfo, Consumer<OcflObjectUpdater> objectUpdater) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectVersionId cannot be null");
        Enforce.notNull(objectUpdater, "objectUpdater cannot be null");

        var inventory = loadInventory(objectVersionId);

        if (inventory == null) {
            // Note: If the mutable HEAD creation fails, the object with the empty version remains
            inventory = createAndPersistEmptyVersion(objectVersionId);
        }

        enforceObjectVersionForUpdate(objectVersionId, inventory);
        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyStateMutable(inventory);

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = FileUtil.createDirectories(resolveRevisionDir(inventory, stagingDir)).getParent();

        try {
            objectUpdater.accept(objectUpdaterBuilder.build(inventory, inventoryUpdater, contentDir));
            var newInventory = buildNewInventory(inventoryUpdater, commitInfo);
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId commitStagedChanges(String objectId, CommitInfo commitInfo) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        var inventory = requireInventory(ObjectVersionId.head(objectId));

        if (inventory.hasMutableHead()) {
            var newInventory = MutableHeadInventoryCommitter.commit(inventory, now(), commitInfo);
            var stagingDir = FileUtil.createTempDir(workDir, objectId);
            writeInventory(newInventory, stagingDir);

            try {
                objectLock.doInWriteLock(inventory.getId(), () -> {
                    try {
                        storage.commitMutableHead(inventory, newInventory, stagingDir);
                        cacheInventory(newInventory);
                    } catch (ObjectOutOfSyncException e) {
                        inventoryCache.invalidate(inventory.getId());
                        throw e;
                    }
                });
            } finally {
                FileUtil.safeDeletePath(stagingDir);
            }
        }

        return ObjectVersionId.version(objectId, inventory.getHead());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeStagedChanges(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        objectLock.doInWriteLock(objectId, () -> {
            try {
                storage.purgeMutableHead(objectId);
            } finally {
                inventoryCache.invalidate(objectId);
            }
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStagedChanges(String objectId) {
        ensureOpen();
        // TODO return false if object does not exist?
        Enforce.notBlank(objectId, "objectId cannot be blank");
        var inventory = requireInventory(ObjectVersionId.head(objectId));
        return inventory.hasMutableHead();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closed = true;
        parallelProcess.shutdown();
        copyParallelProcess.shutdown();
        storage.close();
    }

    private Inventory loadInventory(ObjectVersionId objectId) {
        return objectLock.doInReadLock(objectId.getObjectId(), () ->
                inventoryCache.get(objectId.getObjectId(), storage::loadInventory));
    }

    private void cacheInventory(Inventory inventory) {
        inventoryCache.put(inventory.getId(), inventory);
    }

    private Inventory loadInventoryWithDefault(ObjectVersionId objectId) {
        var inventory = loadInventory(objectId);
        if (inventory == null) {
            inventory = createStubInventory(objectId);
        }
        return inventory;
    }

    private Inventory createStubInventory(ObjectVersionId objectId) {
        var objectRootPath = storage.objectRootPath(objectId.getObjectId());
        return Inventory.stubInventory(objectId.getObjectId(), config, objectRootPath);
    }

    private Inventory requireInventory(ObjectVersionId objectId) {
        var inventory = loadInventory(objectId);
        if (inventory == null) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }
        return inventory;
    }

    private void stageNewVersion(InventoryUpdater inventoryUpdater, Path sourcePath, Path contentDir,
                                 DigestAlgorithm digestAlgorithm, Set<OcflOption> options) {
        var files = FileUtil.findFiles(sourcePath);
        var newFiles = new HashMap<Path, InventoryUpdater.AddFileResult>();

        // TODO very similar logic in ObjectUpdater. extractable?

        var filesWithDigests = parallelProcess.collection(files, file -> {
            var digest = DigestUtil.computeDigest(digestAlgorithm, file);
            return Map.entry(file, digest);
        });

        // Because the InventoryUpdater is not thread safe, this MUST happen synchronously
        for (var fileWithDigest : filesWithDigests) {
            var file = fileWithDigest.getKey();
            var digest = fileWithDigest.getValue();
            var logicalPath = createLogicalPath(sourcePath, file);

            var result = inventoryUpdater.addFile(digest, FileUtil.pathToStringStandardSeparator(logicalPath));

            if (result.isNew()) {
                newFiles.put(file, result);
            }
        }

        copyParallelProcess.map(newFiles, (file, result) -> {
            if (options.contains(OcflOption.MOVE_SOURCE)) {
                FileUtil.moveFileMakeParents(file, contentDir.resolve(result.getPathUnderContentDir()));
            } else {
                FileUtil.copyFileMakeParents(file, contentDir.resolve(result.getPathUnderContentDir()));
            }
        });

        if (options.contains(OcflOption.MOVE_SOURCE)) {
            // Cleanup empty dirs
            FileUtil.safeDeletePath(sourcePath);
        }
    }

    private Inventory buildNewInventory(InventoryUpdater inventoryUpdater, CommitInfo commitInfo) {
        return InventoryValidator.validate(inventoryUpdater.buildNewInventory(now(), commitInfo));
    }

    private Path createLogicalPath(Path sourcePath, Path file) {
        if (Files.isRegularFile(sourcePath) && sourcePath.equals(file)) {
            return file.getFileName();
        }
        return sourcePath.relativize(file);
    }

    private void getObjectInternal(Inventory inventory, VersionId versionId, Path outputPath) {
        var stagingDir = FileUtil.createTempDir(workDir, inventory.getId());

        try {
            storage.reconstructObjectVersion(inventory, versionId, stagingDir);
            FileUtil.moveDirectory(stagingDir, outputPath);
        } catch (FileAlreadyExistsException e) {
            throw new RuntimeIOException(e);
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    private void writeNewVersion(Inventory inventory, Path stagingDir) {
        writeInventory(inventory, stagingDir);
        objectLock.doInWriteLock(inventory.getId(), () -> {
            try {
                storage.storeNewVersion(inventory, stagingDir);
                cacheInventory(inventory);
            } catch (ObjectOutOfSyncException e) {
                inventoryCache.invalidate(inventory.getId());
                throw e;
            }
        });
    }

    private void writeInventory(Inventory inventory, Path stagingDir) {
        try {
            var inventoryPath = ObjectPaths.inventoryPath(stagingDir);
            inventoryMapper.write(inventoryPath, inventory);
            String inventoryDigest = DigestUtil.computeDigest(inventory.getDigestAlgorithm(), inventoryPath);
            Files.writeString(ObjectPaths.inventorySidecarPath(stagingDir, inventory),
                    inventoryDigest + "\t" + OcflConstants.INVENTORY_FILE);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private Inventory createAndPersistEmptyVersion(ObjectVersionId objectId) {
        LOG.info("Creating object {} with an empty version.", objectId.getObjectId());

        var stubInventory = createStubInventory(objectId);
        var stagingDir = FileUtil.createTempDir(workDir, objectId.getObjectId());
        FileUtil.createDirectories(resolveContentDir(stubInventory, stagingDir));

        try {
            var inventoryBuilder = Inventory.builder(stubInventory);
            var inventory = inventoryBuilder
                    .addHeadVersion(Version.builder()
                            .commitInfo(new CommitInfo()
                                    .setMessage("Auto-generated empty object version.")
                                    .setUser(new edu.wisc.library.ocfl.api.model.User().setName("ocfl-java")))
                            .created(now())
                            .build())
                    .build();

            writeNewVersion(inventory, stagingDir);
            return inventory;
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    private void enforceObjectVersionForUpdate(ObjectVersionId objectId, Inventory inventory) {
        if (!objectId.isHead() && !objectId.getVersionId().equals(inventory.getHead())) {
            throw new ObjectOutOfSyncException(String.format("Cannot update object %s because the HEAD version is %s, but version %s was specified.",
                    objectId.getObjectId(), inventory.getHead(), objectId.getVersionId()));
        }
    }

    private void ensureNoMutableHead(Inventory inventory) {
        if (inventory != null && inventory.hasMutableHead()) {
            throw new IllegalStateException(String.format("Cannot create a new version of object %s because it has an active mutable HEAD.",
                    inventory.getId()));
        }
    }

    private VersionId resolveVersion(ObjectVersionId objectId, Inventory inventory) {
        var versionId = inventory.getHead();

        if (!objectId.isHead()) {
            versionId = objectId.getVersionId();
        }

        return versionId;
    }

    private void requireVersion(ObjectVersionId objectId, Inventory inventory) {
        if (objectId.isHead()) {
            return;
        }

        if (inventory.getVersion(objectId.getVersionId()) == null) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectId.getObjectId(), objectId.getVersionId()));
        }
    }

    private Path createStagingDir(ObjectVersionId objectVersionId) {
        return FileUtil.createTempDir(workDir, objectVersionId.getObjectId());
    }

    private Path createStagingContentDir(Inventory inventory, Path stagingDir) {
        return FileUtil.createDirectories(resolveContentDir(inventory, stagingDir));
    }

    private Path resolveContentDir(Inventory inventory, Path parent) {
        return parent.resolve(inventory.resolveContentDirectory());
    }

    private Path resolveRevisionDir(Inventory inventory, Path parent) {
        var contentDir = resolveContentDir(inventory, parent);
        var newRevision = inventory.nextRevisionId();
        return contentDir.resolve(newRevision.toString());
    }

    private VersionDetails createVersionDetails(Inventory inventory, VersionId versionId) {
        var version = inventory.getVersion(versionId);
        return responseMapper.mapVersion(inventory, versionId, version);
    }

    private OffsetDateTime now() {
        // OCFL spec has timestamps reported at second granularity. Unfortunately, it's difficult to make Jackson
        // interact with ISO 8601 at anything other than nanosecond granularity.
        return OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(DefaultOcflRepository.class.getName() + " is closed.");
        }
    }

    /**
     * This is used to manipulate the clock for testing purposes.
     *
     * @param clock clock
     */
    public void setClock(Clock clock) {
        this.clock = Enforce.notNull(clock, "clock cannot be null");
    }

}
