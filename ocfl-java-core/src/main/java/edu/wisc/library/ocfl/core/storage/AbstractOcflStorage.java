package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.storage.OcflStorage;

public abstract class AbstractOcflStorage implements OcflStorage {

    protected InventoryMapper inventoryMapper;
    protected OcflVersion ocflVersion;

    private boolean closed = false;
    private boolean initialized = false;

    @Override
    public synchronized void initializeStorage(OcflVersion ocflVersion, LayoutConfig layoutConfig, InventoryMapper inventoryMapper) {
        if (initialized) {
            return;
        }

        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.ocflVersion = Enforce.notNull(ocflVersion, "ocflVersion cannot be null");

        doInitialize(layoutConfig);
        this.initialized = true;
    }

    @Override
    public void close() {
        closed = true;
    }

    protected abstract void doInitialize(LayoutConfig layoutConfig);

    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(this.getClass().getName() + " is closed.");
        }

        if (!initialized) {
            throw new IllegalStateException(this.getClass().getName() + " must be initialized before it can be used.");
        }
    }

}
