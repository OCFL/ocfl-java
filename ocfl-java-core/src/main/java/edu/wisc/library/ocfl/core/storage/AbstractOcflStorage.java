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

package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.exception.OcflStateException;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;

/**
 * OcflStorage abstract implementation that handles managing the repository's state, initialized, open, close.
 */
public abstract class AbstractOcflStorage implements OcflStorage {

    protected InventoryMapper inventoryMapper;
    protected OcflVersion ocflVersion;
    protected ExtensionSupportEvaluator supportEvaluator;

    private boolean closed = false;
    private boolean initialized = false;
    private RepositoryConfig repositoryConfig;

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized RepositoryConfig initializeStorage(OcflVersion ocflVersion,
                                                           OcflExtensionConfig layoutConfig,
                                                           InventoryMapper inventoryMapper,
                                                           ExtensionSupportEvaluator supportEvaluator) {
        if (this.initialized) {
            return this.repositoryConfig;
        }

        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        this.supportEvaluator = Enforce.notNull(supportEvaluator, "supportEvaluator cannot be null");

        this.repositoryConfig = doInitialize(ocflVersion, layoutConfig);
        this.ocflVersion = Enforce.notNull(this.repositoryConfig.getOcflVersion(), "ocflVersion cannot be null");
        this.initialized = true;
        return repositoryConfig;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        closed = true;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateCache(String objectId) {
        // no op
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void invalidateCache() {
        // no op
    }

    /**
     * Does whatever is necessary to initialize OCFL repository storage.
     *
     * @param ocflVersion the OCFL version, may be null to default to version in storage root
     * @param layoutConfig the storage layout configuration, may be null to auto-detect existing configuration
     */
    protected abstract RepositoryConfig doInitialize(OcflVersion ocflVersion, OcflExtensionConfig layoutConfig);

    /**
     * Throws an exception if the repository has not been initialized or is closed
     */
    protected void ensureOpen() {
        if (closed) {
            throw new OcflStateException(this.getClass().getName() + " is closed.");
        }

        if (!initialized) {
            throw new OcflStateException(this.getClass().getName() + " must be initialized before it can be used.");
        }
    }

}
