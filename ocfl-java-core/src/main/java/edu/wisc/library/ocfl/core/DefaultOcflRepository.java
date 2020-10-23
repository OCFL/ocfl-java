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

import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.FileChangeHistory;
import edu.wisc.library.ocfl.api.model.ObjectDetails;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.OcflObjectVersion;
import edu.wisc.library.ocfl.api.model.OcflObjectVersionFile;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.AddFileProcessor;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.path.ContentPathMapper;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMapper;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import edu.wisc.library.ocfl.core.validation.InventoryValidator;
import edu.wisc.library.ocfl.core.validation.ObjectValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.UncheckedIOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashSet;
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
    protected final Path workDir;
    protected final ObjectLock objectLock;
    protected final ObjectValidator objectValidator;
    protected final ResponseMapper responseMapper;
    protected final InventoryUpdater.Builder inventoryUpdaterBuilder;
    protected final AddFileProcessor.Builder addFileProcessorBuilder;

    protected final OcflConfig config;

    private Clock clock;

    private boolean closed = false;

    /**
     * @see OcflRepositoryBuilder
     *
     * @param storage storage layer
     * @param workDir path to the directory to use for assembling ocfl versions
     * @param objectLock locking client
     * @param inventoryMapper object mapper for serializing inventories
     * @param logicalPathMapper logical path mapper
     * @param contentPathConstraintProcessor content path constraint processor
     * @param config ocfl defaults configuration
     */
    public DefaultOcflRepository(OcflStorage storage, Path workDir,
                                 ObjectLock objectLock,
                                 InventoryMapper inventoryMapper,
                                 LogicalPathMapper logicalPathMapper,
                                 ContentPathConstraintProcessor contentPathConstraintProcessor,
                                 OcflConfig config) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.config = Enforce.notNull(config, "config cannot be null");

        inventoryUpdaterBuilder = InventoryUpdater.builder().contentPathMapperBuilder(
                ContentPathMapper.builder()
                        .logicalPathMapper(logicalPathMapper)
                        .contentPathConstraintProcessor(contentPathConstraintProcessor));

        objectValidator = new ObjectValidator(inventoryMapper);
        responseMapper = new ResponseMapper();
        clock = Clock.systemUTC();

        addFileProcessorBuilder = AddFileProcessor.builder();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId putObject(ObjectVersionId objectVersionId, Path path, VersionInfo versionInfo, OcflOption... ocflOptions) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        Enforce.notNull(path, "path cannot be null");

        LOG.debug("Putting object at <{}> into OCFL repo under id <{}>", path, objectVersionId.getObjectId());

        var inventory = loadInventoryWithDefault(objectVersionId);
        ensureNoMutableHead(inventory);
        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var inventoryUpdater = inventoryUpdaterBuilder.buildBlankState(inventory);

        var stagingDir = createStagingDir(objectVersionId.getObjectId());
        var contentDir = createStagingContentDir(inventory, stagingDir);

        var fileProcessor = addFileProcessorBuilder.build(inventoryUpdater, contentDir, inventory.getDigestAlgorithm());
        fileProcessor.processPath(path, ocflOptions);

        var newInventory = buildNewInventory(inventoryUpdater, versionInfo);

        try {
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId updateObject(ObjectVersionId objectVersionId, VersionInfo versionInfo, Consumer<OcflObjectUpdater> objectUpdater) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        Enforce.notNull(objectUpdater, "objectUpdater cannot be null");

        LOG.debug("Update object <{}>", objectVersionId.getObjectId());

        var inventory = loadInventoryWithDefault(objectVersionId);
        ensureNoMutableHead(inventory);
        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var stagingDir = createStagingDir(objectVersionId.getObjectId());
        var contentDir = createStagingContentDir(inventory, stagingDir);

        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyState(inventory);
        var addFileProcessor = addFileProcessorBuilder.build(inventoryUpdater, contentDir, inventory.getDigestAlgorithm());
        var updater = new DefaultOcflObjectUpdater(inventory, inventoryUpdater, contentDir, addFileProcessor);

        try {
            objectUpdater.accept(updater);
            var newInventory = buildNewInventory(inventoryUpdater, versionInfo);
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getObject(ObjectVersionId objectVersionId, Path outputPath) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        ensureOutputPath(outputPath);

        LOG.debug("Get object <{}> and copy to <{}>", objectVersionId, outputPath);

        var inventory = requireInventory(objectVersionId);
        var versionId = requireVersion(objectVersionId, inventory);

        getObjectInternal(inventory, versionId, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectVersion getObject(ObjectVersionId objectVersionId) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");

        LOG.debug("Get object <{}>", objectVersionId);

        var inventory = requireInventory(objectVersionId);
        var versionId = requireVersion(objectVersionId, inventory);

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

        LOG.debug("Describe object <{}>", objectId);

        var inventory = requireInventory(ObjectVersionId.head(objectId));

        return responseMapper.mapInventory(inventory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionDetails describeVersion(ObjectVersionId objectVersionId) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectVersionId cannot be null");

        LOG.debug("Describe version <{}>", objectVersionId);

        var inventory = requireInventory(objectVersionId);
        var versionId = requireVersion(objectVersionId, inventory);

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

        LOG.debug("Get file change history for object <{}> logical path <{}>", objectId, logicalPath);

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

        LOG.debug("Contains object <{}>", objectId);

        return storage.containsObject(objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        LOG.info("Purge object <{}>", objectId);

        objectLock.doInWriteLock(objectId, () -> storage.purgeObject(objectId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId replicateVersionAsHead(ObjectVersionId objectVersionId, VersionInfo versionInfo) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectVersionId cannot be null");

        LOG.debug("Replicate version <{}>", objectVersionId);

        var inventory = requireInventory(objectVersionId);
        var versionId = requireVersion(objectVersionId, inventory);

        ensureNoMutableHead(inventory);

        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyState(inventory, versionId);
        var newInventory = inventoryUpdater.buildNewInventory(now(versionInfo), versionInfo);

        var stagingDir = createStagingDir(objectVersionId.getObjectId());
        // content dir is not used but must exist
        createStagingContentDir(inventory, stagingDir);

        try {
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void rollbackToVersion(ObjectVersionId objectVersionId) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectVersionId cannot be null");

        LOG.info("Rollback to version <{}>", objectVersionId);

        var inventory = requireInventory(objectVersionId);
        var versionId = requireVersion(objectVersionId, inventory);

        if (versionId == inventory.getHead()) {
            LOG.debug("Object {} cannot be rollback to version {} because it is already the head version.",
                    objectVersionId.getObjectId(), versionId);
            return;
        }

        objectLock.doInWriteLock(inventory.getId(), () -> storage.rollbackToVersion(inventory, versionId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Stream<String> listObjectIds() {
        ensureOpen();

        LOG.debug("List object ids");

        return storage.listObjectIds();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportVersion(ObjectVersionId objectVersionId, Path outputPath) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectId cannot be null");
        ensureExportPath(outputPath);

        var exportId = objectVersionId;

        if (objectVersionId.isHead()) {
            var inventory = requireInventory(objectVersionId);
            var headVersion = inventory.getHead();
            exportId = ObjectVersionId.version(objectVersionId.getObjectId(), headVersion);
        }

        LOG.debug("Export <{}> to <{}>", objectVersionId, outputPath);

        storage.exportVersion(exportId, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void exportObject(String objectId, Path outputPath) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");
        ensureExportPath(outputPath);

        LOG.debug("Export <{}> to <{}>", objectId, outputPath);

        objectLock.doInWriteLock(objectId, () -> storage.exportObject(objectId, outputPath));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void importVersion(Path versionPath, OcflOption... ocflOptions) {
        ensureOpen();

        Enforce.notNull(versionPath, "versionPath cannot be null");

        var importInventory = createImportVersionInventory(versionPath);

        InventoryValidator.validateShallow(importInventory);
        objectValidator.validateVersion(versionPath, importInventory);

        // TODO Existing bug: What if a version is removed immediately before a new version is added that is based on the new removed version?

        var stagingDir = createStagingDir(importInventory.getId());

        try {
            importToStaging(versionPath, stagingDir, ocflOptions);
            objectLock.doInWriteLock(importInventory.getId(), () -> storage.storeNewVersion(importInventory, stagingDir));
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void importObject(Path objectPath, OcflOption... ocflOptions) {
        ensureOpen();

        Enforce.notNull(objectPath, "objectPath cannot be null");

        var inventoryPath = ObjectPaths.inventoryPath(objectPath);

        Enforce.expressionTrue(Files.exists(inventoryPath), inventoryPath, "inventory.json must exist");

        var inventory = inventoryMapper.read(objectPath.toString(), inventoryPath);
        var objectId = inventory.getId();

        if (containsObject(objectId)) {
            throw new IllegalStateException(String.format("Cannot import object at %s because an object already exists with ID %s.",
                    objectPath, objectId));
        }

        objectValidator.validateObject(objectPath, inventory);

        var stagingDir = createStagingDir(objectId);

        try {
            importToStaging(objectPath, stagingDir, ocflOptions);
            objectLock.doInWriteLock(objectId, () -> storage.importObject(objectId, stagingDir));
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOG.debug("Close OCFL repository");

        closed = true;
        storage.close();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflConfig config() {
        return new OcflConfig(config);
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

    protected Inventory buildNewInventory(InventoryUpdater inventoryUpdater, VersionInfo versionInfo) {
        return InventoryValidator.validateShallow(inventoryUpdater.buildNewInventory(now(versionInfo), versionInfo));
    }

    private void getObjectInternal(Inventory inventory, VersionId versionId, Path outputPath) {
        var stagingDir = createStagingDir(inventory.getId());

        try {
            storage.reconstructObjectVersion(inventory, versionId, stagingDir);
            FileUtil.moveDirectory(stagingDir, outputPath);
        } catch (FileAlreadyExistsException e) {
            throw new UncheckedIOException(e);
        } finally {
            FileUtil.safeDeleteDirectory(stagingDir);
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
        SidecarMapper.writeSidecar(inventory, inventoryDigest, stagingDir);
    }

    private Inventory createImportVersionInventory(Path versionPath) {
        var inventoryPath = ObjectPaths.inventoryPath(versionPath);

        Enforce.expressionTrue(Files.exists(inventoryPath), inventoryPath, "inventory.json must exist");

        var importInventory = inventoryMapper.read(versionPath.toString(), inventoryPath);
        var objectId = importInventory.getId();

        var existingInventory = loadInventory(ObjectVersionId.head(objectId));

        ensureNoMutableHead(existingInventory);

        if (existingInventory == null) {
            if (!VersionId.V1.equals(importInventory.getHead())) {
                throw new IllegalStateException(String.format("Cannot import object %s version %s from source %s." +
                                " The import version must be the next sequential version, and the object doest not currently exist.",
                        objectId, importInventory.getHead(), versionPath));
            }
        } else {
            if (!existingInventory.getHead().nextVersionId().equals(importInventory.getHead())) {
                throw new IllegalStateException(String.format("Cannot import object %s version %s from source %s." +
                                " The import version must be the next sequential version, and the current version is %s.",
                        objectId, importInventory.getHead(), versionPath, existingInventory.getHead()));
            }

            InventoryValidator.validateCompatibleInventories(importInventory, existingInventory);
        }

        var objectRootPath = storage.objectRootPath(objectId);
        return Inventory.builder(importInventory)
                .objectRootPath(objectRootPath)
                .build();
    }

    private void importToStaging(Path source, Path stagingDir, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));

        if (options.contains(OcflOption.MOVE_SOURCE)) {
            // Delete the staging directory so that the move operation works
            UncheckedFiles.delete(stagingDir);
            try {
                FileUtil.moveDirectory(source, stagingDir);
            } catch (FileAlreadyExistsException e) {
                throw new UncheckedIOException(e);
            }
        } else {
            // TODO do we care about parallelizing this?
            FileUtil.recursiveCopy(source, stagingDir);
        }
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

    private VersionId requireVersion(ObjectVersionId objectId, Inventory inventory) {
        if (objectId.isHead()) {
            return inventory.getHead();
        }

        if (inventory.getVersion(objectId.getVersionId()) == null) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectId.getObjectId(), objectId.getVersionId()));
        }

        return objectId.getVersionId();
    }

    protected Path createStagingDir(String objectId) {
        return FileUtil.createObjectTempDir(workDir, objectId);
    }

    private Path createStagingContentDir(Inventory inventory, Path stagingDir) {
        return UncheckedFiles.createDirectories(resolveContentDir(inventory, stagingDir));
    }

    protected Path resolveContentDir(Inventory inventory, Path parent) {
        return parent.resolve(inventory.resolveContentDirectory());
    }

    private VersionDetails createVersionDetails(Inventory inventory, VersionId versionId) {
        var version = inventory.getVersion(versionId);
        return responseMapper.mapVersion(inventory, versionId, version);
    }

    protected OffsetDateTime now(VersionInfo versionInfo) {
        if (versionInfo != null && versionInfo.getCreated() != null) {
            return versionInfo.getCreated();
        }
        return OffsetDateTime.now(clock);
    }

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(DefaultOcflRepository.class.getName() + " is closed.");
        }
    }

    private void ensureOutputPath(Path outputPath) {
        Enforce.notNull(outputPath, "outputPath cannot be null");
        Enforce.expressionTrue(Files.notExists(outputPath), outputPath, "outputPath must not exist");
        Enforce.expressionTrue(Files.exists(outputPath.getParent()), outputPath, "outputPath parent must exist");
        Enforce.expressionTrue(Files.isDirectory(outputPath.getParent()), outputPath, "outputPath parent must be a directory");
    }

    private void ensureExportPath(Path outputPath) {
        Enforce.notNull(outputPath, "outputPath cannot be null");
        if (Files.exists(outputPath)) {
            Enforce.expressionTrue(Files.isDirectory(outputPath), outputPath, "outputPath must be a directory");
        }
        UncheckedFiles.createDirectories(outputPath);
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
