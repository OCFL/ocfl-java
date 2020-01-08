package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;

/**
 * OcflStorage abstract implementation that handles managing the repository's state, initialized, open, close.
 */
public abstract class AbstractOcflStorage implements OcflStorage {

    protected InventoryMapper inventoryMapper;
    protected OcflVersion ocflVersion;

    private boolean closed = false;
    private boolean initialized = false;

    /**
     * {@inheritDoc}
     */
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

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * Does whatever is necessary to initialize OCFL repository storage.
     *
     * @param layoutConfig the storage layout configuration, may be null to auto-detect existing configuration
     */
    protected abstract void doInitialize(LayoutConfig layoutConfig);

    /**
     * Throws an exception if the repository has not been initialized or is closed
     */
    protected void ensureOpen() {
        if (closed) {
            throw new IllegalStateException(this.getClass().getName() + " is closed.");
        }

        if (!initialized) {
            throw new IllegalStateException(this.getClass().getName() + " must be initialized before it can be used.");
        }
    }

}
