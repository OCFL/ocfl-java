package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.model.Inventory;

import java.nio.file.Path;
import java.util.Map;

public class CachingOcflStorage extends AbstractOcflStorage {

    private Cache<String, Inventory> inventoryCache;
    private OcflStorage delegate;

    public CachingOcflStorage(Cache<String, Inventory> inventoryCache, OcflStorage delegate) {
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        this.delegate = Enforce.notNull(delegate, "delegate cannot be null");
    }

    @Override
    protected void doInitialize(LayoutConfig layoutConfig) {
        delegate.initializeStorage(ocflVersion, layoutConfig, inventoryMapper);
    }

    @Override
    public Inventory loadInventory(String objectId) {
        ensureOpen();

        return inventoryCache.get(objectId, delegate::loadInventory);
    }

    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        ensureOpen();

        try {
            delegate.storeNewVersion(inventory, stagingDir);
            inventoryCache.put(inventory.getId(), inventory);
        } catch (ObjectOutOfSyncException e) {
            inventoryCache.invalidate(inventory.getId());
            throw e;
        }
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

        try {
            delegate.purgeObject(objectId);
        } finally {
            inventoryCache.invalidate(objectId);
        }
    }

    @Override
    public void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir) {
        ensureOpen();

        try {
            delegate.commitMutableHead(oldInventory, newInventory, stagingDir);
            inventoryCache.put(newInventory.getId(), newInventory);
        } catch (ObjectOutOfSyncException e) {
            inventoryCache.invalidate(newInventory.getId());
            throw e;
        }
    }

    @Override
    public void purgeMutableHead(String objectId) {
        ensureOpen();

        try {
            delegate.purgeMutableHead(objectId);
        } finally {
            inventoryCache.invalidate(objectId);
        }
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
}
