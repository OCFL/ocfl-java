package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectReader;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectDetails;
import edu.wisc.library.ocfl.api.model.ObjectId;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryType;
import edu.wisc.library.ocfl.core.model.VersionId;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.InventoryMapper;
import edu.wisc.library.ocfl.core.util.ResponseMapper;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.OffsetDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Set;
import java.util.function.Consumer;

/**
 * Primary implementation of the OcflRepository API. It is storage agnostic. It is typically instantiated using
 * OcflRepositoryBuilder.
 *
 * @see OcflRepositoryBuilder
 */
public class DefaultOcflRepository implements OcflRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflRepository.class);

    private OcflStorage storage;
    private InventoryMapper inventoryMapper;
    private Path workDir;
    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private ResponseMapper responseMapper;

    private Set<DigestAlgorithm> fixityAlgorithms;
    private InventoryType inventoryType;
    private DigestAlgorithm digestAlgorithm;
    private String contentDirectory;

    private Clock clock;

    public DefaultOcflRepository(OcflStorage storage, Path workDir,
                                 ObjectLock objectLock,
                                 Cache<String, Inventory> inventoryCache,
                                 InventoryMapper inventoryMapper,
                                 Set<DigestAlgorithm> fixityAlgorithms,
                                 InventoryType inventoryType, DigestAlgorithm digestAlgorithm,
                                 String contentDirectory) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.fixityAlgorithms = Enforce.notNull(fixityAlgorithms, "fixityAlgorithms cannot be null");
        this.inventoryType = Enforce.notNull(inventoryType, "inventoryType cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");

        responseMapper = new ResponseMapper();
        clock = Clock.systemUTC();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectId putObject(ObjectId objectId, Path path, CommitInfo commitInfo) {
        Enforce.notNull(objectId, "objectId cannot be null");
        Enforce.notNull(path, "path cannot be null");

        var inventory = loadInventory(objectId);
        var contentDir = resolveContentDir(inventory);
        var updater = createInventoryUpdater(objectId, inventory, true);

        // Only needs to be cleaned on failure
        var stagingDir = FileUtil.createTempDir(workDir, objectId.getObjectId());
        var newInventory = stageNewVersion(updater, path, commitInfo, stagingDir, contentDir);

        try {
            writeNewVersion(newInventory, stagingDir);
            return ObjectId.version(objectId.getObjectId(), newInventory.getHead().toString());
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(stagingDir);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectId updateObject(ObjectId objectId, CommitInfo commitInfo, Consumer<OcflObjectUpdater> objectUpdater) {
        Enforce.notNull(objectId, "objectId cannot be null");
        Enforce.notNull(objectUpdater, "objectUpdater cannot be null");

        var inventory = loadInventory(objectId);
        var updater = createInventoryUpdater(objectId, inventory, false);
        updater.addCommitInfo(commitInfo);

        // Only needs to be cleaned on failure
        var stagingDir = FileUtil.createTempDir(workDir, objectId.getObjectId());
        var contentDir = FileUtil.createDirectories(stagingDir.resolve(resolveContentDir(inventory)));

        try {
            objectUpdater.accept(new DefaultOcflObjectUpdater(updater, contentDir));
            var newInventory = updater.finalizeUpdate();
            writeNewVersion(newInventory, stagingDir);
            return ObjectId.version(objectId.getObjectId(), newInventory.getHead().toString());
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(stagingDir);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void getObject(ObjectId objectId, Path outputPath) {
        Enforce.notNull(objectId, "objectId cannot be null");
        Enforce.notNull(outputPath, "outputPath cannot be null");
        Enforce.expressionTrue(Files.exists(outputPath), outputPath, "outputPath must exist");
        Enforce.expressionTrue(Files.isDirectory(outputPath), outputPath, "outputPath must be a directory");

        var inventory = requireInventory(objectId);

        requireVersion(objectId, inventory);
        var versionId = resolveVersion(objectId, inventory);

        getObjectInternal(inventory, versionId, outputPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void readObject(ObjectId objectId, Consumer<OcflObjectReader> objectReader) {
        Enforce.notNull(objectId, "objectId cannot be null");
        Enforce.notNull(objectReader, "objectReader cannot be null");

        var inventory = requireInventory(objectId);

        requireVersion(objectId, inventory);

        var stagingDir = FileUtil.createTempDir(workDir, inventory.getId());
        var versionId = resolveVersion(objectId, inventory);

        try (var reader = new DefaultOcflObjectReader(
                storage, inventory, versionId, stagingDir)) {
            objectReader.accept(reader);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectDetails describeObject(String objectId) {
        Enforce.notBlank(objectId, "objectId cannot be blank");

        var inventory = requireInventory(ObjectId.head(objectId));

        return responseMapper.mapInventory(inventory);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public VersionDetails describeVersion(ObjectId objectId) {
        Enforce.notNull(objectId, "objectId cannot be null");

        var inventory = requireInventory(objectId);
        requireVersion(objectId, inventory);

        var version = inventory.getVersion(VersionId.fromValue(objectId.getVersionId()));

        return responseMapper.mapVersion(inventory, objectId.getVersionId(), version);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean containsObject(String objectId) {
        Enforce.notNull(objectId, "objectId cannot be null");
        return storage.containsObject(objectId);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeObject(String objectId) {
        Enforce.notBlank(objectId, "objectId cannot be blank");

        objectLock.doInWriteLock(objectId, () -> {
            try {
                storage.purgeObject(objectId);
            } finally {
                inventoryCache.invalidate(objectId);
            }
        });
    }

    private Inventory loadInventory(ObjectId objectId) {
        return objectLock.doInReadLock(objectId.getObjectId(), () ->
                inventoryCache.get(objectId.getObjectId(), storage::loadInventory));
    }

    private void cacheInventory(Inventory inventory) {
        inventoryCache.put(inventory.getId(), inventory);
    }

    private Inventory requireInventory(ObjectId objectId) {
        var inventory = loadInventory(objectId);
        if (inventory == null) {
            throw new NotFoundException(String.format("Object %s was not found.", objectId));
        }
        return inventory;
    }

    private InventoryUpdater createInventoryUpdater(ObjectId objectId, Inventory inventory, boolean isInsert) {
        InventoryUpdater updater;

        if (inventory != null) {
            enforceObjectVersionForUpdate(objectId, inventory);
            if (isInsert) {
                updater = InventoryUpdater.newVersionForInsert(inventory, fixityAlgorithms, now());
            } else {
                updater = InventoryUpdater.newVersionForUpdate(inventory, fixityAlgorithms, now());
            }
        } else {
            updater = InventoryUpdater.newInventory(
                    objectId.getObjectId(),
                    inventoryType,
                    digestAlgorithm,
                    this.contentDirectory,
                    fixityAlgorithms,
                    now());
        }

        return updater;
    }

    private Inventory stageNewVersion(InventoryUpdater updater, Path sourcePath, CommitInfo commitInfo, Path stagingDir, String contentDirectory) {
        updater.addCommitInfo(commitInfo);

        var files = FileUtil.findFiles(sourcePath);
        var contentDir = FileUtil.createDirectories(stagingDir.resolve(contentDirectory));

        for (var file : files) {
            var relativePath = sourcePath.relativize(file);
            var isNewFile = updater.addFile(file, relativePath);

            if (isNewFile) {
                FileUtil.copyFileMakeParents(file, contentDir.resolve(relativePath));
            }
        }

        return updater.finalizeUpdate();
    }

    private void getObjectInternal(Inventory inventory, VersionId versionId, Path outputPath) {
        // Only needs to be cleaned on failure
        var stagingDir = FileUtil.createTempDir(workDir, inventory.getId());

        try {
            storage.reconstructObjectVersion(inventory, versionId, stagingDir);

            FileUtil.moveDirectory(stagingDir, outputPath);
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(stagingDir);
            throw e;
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
            var inventoryPath = stagingDir.resolve(OcflConstants.INVENTORY_FILE);
            inventoryMapper.writeValue(inventoryPath, inventory);
            String inventoryDigest = computeDigest(inventoryPath, inventory.getDigestAlgorithm());
            Files.writeString(
                    stagingDir.resolve(OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getValue()),
                    inventoryDigest + "\t" + OcflConstants.INVENTORY_FILE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void enforceObjectVersionForUpdate(ObjectId objectId, Inventory inventory) {
        if (!objectId.isHead() && !objectId.getVersionId().equals(inventory.getHead().toString())) {
            throw new ObjectOutOfSyncException(String.format("Cannot update object %s because the HEAD version is %s, but version %s was specified.",
                    objectId.getObjectId(), inventory.getHead(), objectId.getVersionId()));
        }
    }


    private VersionId resolveVersion(ObjectId objectId, Inventory inventory) {
        var versionId = inventory.getHead();

        if (!objectId.isHead()) {
            versionId = VersionId.fromValue(objectId.getVersionId());
        }

        return versionId;
    }

    private void requireVersion(ObjectId objectId, Inventory inventory) {
        if (objectId.isHead()) {
            return;
        }

        if (inventory.getVersion(VersionId.fromValue(objectId.getVersionId())) == null) {
            throw new NotFoundException(String.format("Object %s version %s was not found.",
                    objectId.getObjectId(), objectId.getVersionId()));
        }
    }

    private String resolveContentDir(Inventory inventory) {
        if (inventory != null) {
            return inventory.getContentDirectory();
        }
        return this.contentDirectory;
    }

    private OffsetDateTime now() {
        // OCFL spec has timestamps reported at second granularity. Unfortunately, it's difficult to make Jackson
        // interact with ISO 8601 at anything other than nanosecond granularity.
        return OffsetDateTime.now(clock).truncatedTo(ChronoUnit.SECONDS);
    }

    /**
     * This is used to manipulate the clock for testing purposes.
     */
    public void setClock(Clock clock) {
        this.clock = Enforce.notNull(clock, "clock cannot be null");
    }

    public DefaultOcflRepository setInventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

}
