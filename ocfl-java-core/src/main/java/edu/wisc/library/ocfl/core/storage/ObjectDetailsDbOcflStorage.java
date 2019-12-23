package edu.wisc.library.ocfl.core.storage;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.db.OcflObjectDatabase;
import edu.wisc.library.ocfl.core.db.OcflObjectDetails;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.file.Path;
import java.security.DigestOutputStream;
import java.util.Map;

public class ObjectDetailsDbOcflStorage extends AbstractOcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(ObjectDetailsDbOcflStorage.class);

    private OcflObjectDatabase objectDetailsDb;
    private OcflStorage delegate;
    private SidecarMapper sidecarMapper;

    public ObjectDetailsDbOcflStorage(OcflObjectDatabase objectDetailsDb, OcflStorage delegate) {
        this.objectDetailsDb = Enforce.notNull(objectDetailsDb, "objectDetailsDb cannot be null");
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
        this.sidecarMapper = new SidecarMapper();
    }

    @Override
    protected void doInitialize(LayoutConfig layoutConfig) {
        delegate.initializeStorage(ocflVersion, layoutConfig, inventoryMapper);
    }

    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        var details = objectDetailsDb.retrieveObjectDetails(objectId);
        if (details == null || details.getInventoryBytes() == null) {
            var inventory = delegate.loadInventory(objectId);

            if (inventory != null) {
                var baos = new ByteArrayOutputStream();
                var stream = new DigestOutputStream(baos, inventory.getDigestAlgorithm().getMessageDigest());
                inventoryMapper.write(stream, inventory);
                var digest = Bytes.wrap(stream.getMessageDigest().digest()).encodeHex(false);
                objectDetailsDb.addObjectDetails(inventory, digest, baos.toByteArray());
            }

            return inventory;
        }
        return parseInventory(details);
    }

    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

        updateDetails(inventory, stagingDir, () -> delegate.storeNewVersion(inventory, stagingDir));
    }

    @Override
    public Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId) {
        ensureOpen();

        return delegate.getObjectStreams(inventory, versionId);
    }

    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {
        ensureOpen();

        delegate.reconstructObjectVersion(inventory, versionId, stagingDir);
    }

    @Override
    public void purgeObject(String objectId) {
        ensureOpen();

        delegate.purgeObject(objectId);
        objectDetailsDb.deleteObjectDetails(objectId);
    }

    @Override
    public void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir) {
        ensureOpen();

        updateDetails(newInventory, stagingDir, () -> delegate.commitMutableHead(oldInventory, newInventory, stagingDir));
    }

    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        delegate.purgeMutableHead(objectId);
        objectDetailsDb.deleteObjectDetails(objectId);
    }

    @Override
    public boolean containsObject(String objectId) {
        ensureOpen();

        return delegate.containsObject(objectId);
    }

    @Override
    public String objectRootPath(String objectId) {
        ensureOpen();

        return delegate.objectRootPath(objectId);
    }

    @Override
    public void close() {
        delegate.close();
    }

    private void updateDetails(Inventory inventory, Path stagingDir, Runnable runnable) {
        var inventoryPath = ObjectPaths.inventoryPath(stagingDir);
        var sidecarPath = ObjectPaths.inventorySidecarPath(stagingDir, inventory);
        var digest = sidecarMapper.readSidecar(sidecarPath);
        try {
            objectDetailsDb.updateObjectDetails(inventory, digest, inventoryPath, runnable);
        } catch (ObjectOutOfSyncException e) {
            // TODO could this cause problems if there was something waiting for the lock?
            try {
                objectDetailsDb.deleteObjectDetails(inventory.getId());
            } catch (RuntimeException e1) {
                LOG.warn("Failed to remove object details for object {} from DB", inventory.getId(), e1);
            }
            throw e;
        }
    }

    private Inventory parseInventory(OcflObjectDetails details) {
        var actualDigest = DigestUtil.computeDigestHex(details.getDigestAlgorithm(), details.getInventoryBytes());

        if (!details.getInventoryDigest().equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Expected %s digest: %s; Actual: %s",
                    details.getDigestAlgorithm(), details.getInventoryDigest(), actualDigest));
        }

        if (details.getRevisionId() == null) {
            return inventoryMapper.read(details.getObjectRootPath(), new ByteArrayInputStream(details.getInventoryBytes()));
        } else {
            return inventoryMapper.readMutableHead(details.getObjectRootPath(), details.getRevisionId(), new ByteArrayInputStream(details.getInventoryBytes()));
        }
    }

}
