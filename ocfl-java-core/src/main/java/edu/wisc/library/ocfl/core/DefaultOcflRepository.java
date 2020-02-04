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

package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.*;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.*;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ExecutorTerminator;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.inventory.*;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.path.ContentPathMapper;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.QuietFiles;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Primary implementation of the OcflRepository API. It is storage agnostic. It is typically instantiated using
 * OcflRepositoryBuilder.
 *
 * @see OcflRepositoryBuilder
 */
public class DefaultOcflRepository implements OcflRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflRepository.class);

    protected final OcflStorage storage;
    protected final InventoryMapper inventoryMapper;
    protected final SidecarMapper sidecarMapper;
    protected final Path workDir;
    protected final ObjectLock objectLock;
    protected final ResponseMapper responseMapper;
    protected final InventoryUpdater.Builder inventoryUpdaterBuilder;
    protected final AddFileProcessor.Builder addFileProcessorBuilder;

    protected final OcflConfig config;

    private final ParallelProcess parallelProcess;
    private final ParallelProcess copyParallelProcess;

    private Clock clock;

    private boolean closed = false;

    /**
     * @see OcflRepositoryBuilder
     *
     * @param storage storage layer
     * @param workDir path to the directory to use for assembling ocfl versions
     * @param objectLock locking client
     * @param inventoryMapper object mapper for serializing inventories
     * @param pathSanitizer content path sanitizer
     * @param contentPathConstraintProcessor content path constraint processor
     * @param config ocfl defaults configuration
     * @param digestThreadPoolSize number of threads to use for computing digests
     * @param copyThreadPoolSize number of threads to use for copying files
     */
    public DefaultOcflRepository(OcflStorage storage, Path workDir,
                                 ObjectLock objectLock,
                                 InventoryMapper inventoryMapper,
                                 PathSanitizer pathSanitizer,
                                 ContentPathConstraintProcessor contentPathConstraintProcessor,
                                 OcflConfig config,
                                 int digestThreadPoolSize, int copyThreadPoolSize) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.config = Enforce.notNull(config, "config cannot be null");

        Enforce.expressionTrue(digestThreadPoolSize > 0, digestThreadPoolSize, "digestThreadPoolSize must be greater than 0");
        Enforce.expressionTrue(copyThreadPoolSize > 0, copyThreadPoolSize, "copyThreadPoolSize must be greater than 0");

        inventoryUpdaterBuilder = InventoryUpdater.builder().contentPathMapperBuilder(
                ContentPathMapper.builder()
                        .pathSanitizer(pathSanitizer)
                        .contentPathConstraintProcessor(contentPathConstraintProcessor));

        sidecarMapper = new SidecarMapper();
        responseMapper = new ResponseMapper();
        clock = Clock.systemUTC();

        parallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(digestThreadPoolSize)));
        copyParallelProcess = new ParallelProcess(ExecutorTerminator.addShutdownHook(Executors.newFixedThreadPool(copyThreadPoolSize)));

        addFileProcessorBuilder = AddFileProcessor.builder()
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

        var inventory = loadInventoryWithDefault(objectVersionId);
        ensureNoMutableHead(inventory);
        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var inventoryUpdater = inventoryUpdaterBuilder.buildBlankState(inventory);

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = createStagingContentDir(inventory, stagingDir);

        var fileProcessor = addFileProcessorBuilder.build(inventoryUpdater, contentDir, inventory.getDigestAlgorithm());
        fileProcessor.processPath(path, ocflOptions);

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

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = createStagingContentDir(inventory, stagingDir);

        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyState(inventory);
        var addFileProcessor = addFileProcessorBuilder.build(inventoryUpdater, contentDir, inventory.getDigestAlgorithm());
        var updater = new DefaultOcflObjectUpdater(inventory, inventoryUpdater, contentDir, addFileProcessor);

        try {
            objectUpdater.accept(updater);
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
    public FileChangeHistory fileChangeHistory(String objectId, String logicalPath) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");
        Enforce.notBlank(logicalPath, "logicalPath cannot be blank");

        var inventory = requireInventory(ObjectVersionId.head(objectId));
        var changeHistory = responseMapper.fileChangeHistory(inventory, logicalPath);

        if (changeHistory.getFileChanges().isEmpty()) {
            throw new NotFoundException(String.format("The logical path %s was not found in object %s.", logicalPath, objectId));
        }

        return changeHistory;
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

        objectLock.doInWriteLock(objectId, () -> storage.purgeObject(objectId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<String> listObjectIds() {
        ensureOpen();

        return storage.listObjectIds();
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

    protected Inventory loadInventory(ObjectVersionId objectId) {
        return storage.loadInventory(objectId.getObjectId());
    }

    private Inventory loadInventoryWithDefault(ObjectVersionId objectId) {
        var inventory = loadInventory(objectId);
        if (inventory == null) {
            inventory = createStubInventory(objectId);
        }
        return inventory;
    }

    protected Inventory createStubInventory(ObjectVersionId objectId) {
        var objectRootPath = storage.objectRootPath(objectId.getObjectId());
        return Inventory.stubInventory(objectId.getObjectId(), config, objectRootPath);
    }

    protected Inventory requireInventory(ObjectVersionId objectId) {
        var inventory = loadInventory(objectId);
        if (inventory == null) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }
        return inventory;
    }

    protected Inventory buildNewInventory(InventoryUpdater inventoryUpdater, CommitInfo commitInfo) {
        return InventoryValidator.validate(inventoryUpdater.buildNewInventory(now(), commitInfo));
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

    protected void writeNewVersion(Inventory inventory, Path stagingDir) {
        writeInventory(inventory, stagingDir);
        objectLock.doInWriteLock(inventory.getId(), () -> storage.storeNewVersion(inventory, stagingDir));
    }

    protected void writeInventory(Inventory inventory, Path stagingDir) {
        var inventoryPath = ObjectPaths.inventoryPath(stagingDir);
        inventoryMapper.write(inventoryPath, inventory);
        String inventoryDigest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), inventoryPath);
        sidecarMapper.writeSidecar(inventory, inventoryDigest, stagingDir);
    }

    protected void enforceObjectVersionForUpdate(ObjectVersionId objectId, Inventory inventory) {
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

    protected Path createStagingDir(ObjectVersionId objectVersionId) {
        return FileUtil.createTempDir(workDir, objectVersionId.getObjectId());
    }

    private Path createStagingContentDir(Inventory inventory, Path stagingDir) {
        return QuietFiles.createDirectories(resolveContentDir(inventory, stagingDir));
    }

    protected Path resolveContentDir(Inventory inventory, Path parent) {
        return parent.resolve(inventory.resolveContentDirectory());
    }

    private VersionDetails createVersionDetails(Inventory inventory, VersionId versionId) {
        var version = inventory.getVersion(versionId);
        return responseMapper.mapVersion(inventory, versionId, version);
    }

    protected OffsetDateTime now() {
        // OCFL spec has timestamps reported at second granularity. Unfortunately, it's difficult to make Jackson
        // interact with ISO 8601 at anything other than nanosecond granularity.
        return OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
    }

    protected void ensureOpen() {
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
