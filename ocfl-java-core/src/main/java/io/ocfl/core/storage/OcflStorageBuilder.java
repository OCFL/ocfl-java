/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.storage.cloud.CloudClient;
import io.ocfl.core.storage.cloud.CloudStorage;
import io.ocfl.core.storage.common.Storage;
import io.ocfl.core.storage.filesystem.FileSystemStorage;
import io.ocfl.core.util.ObjectMappers;
import java.nio.file.Path;

/**
 * Builder for constructing {@link OcflStorage} objects. It is configured with sensible defaults and can minimally be
 * used as {@code OcflStorageBuilder.builder().fileSystem(storageRoot).build()}.
 */
public class OcflStorageBuilder {

    private ObjectMapper objectMapper;
    private Storage storage;
    private OcflStorageInitializer initializer;
    private boolean verifyInventoryDigest;

    public static OcflStorageBuilder builder() {
        return new OcflStorageBuilder();
    }

    public OcflStorageBuilder() {
        objectMapper = ObjectMappers.prettyPrintMapper();
        this.verifyInventoryDigest = true;
    }

    /**
     * Set the storage implementation to use. This method, {@link #fileSystem(Path)}, or {@link #cloud(CloudClient)}
     * must be used.
     *
     * @param storage storage implementation
     * @return builder
     */
    public OcflStorageBuilder storage(Storage storage) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        return this;
    }

    /**
     * Configure local filesystem based storage implementation. This method, {@link #storage(Storage)}, or {@link #cloud(CloudClient)}
     * must be used.
     *
     * @param storageRoot path to the OCFL storage root directory
     * @return builder
     */
    public OcflStorageBuilder fileSystem(Path storageRoot) {
        this.storage = new FileSystemStorage(storageRoot);
        return this;
    }

    /**
     * Configure cloud based storage implementation. This method, {@link #storage(Storage)}, or {@link #fileSystem(Path)}
     * must be used.
     *
     * @param cloudClient client to use to connect to the cloud storage
     * @return builder
     */
    public OcflStorageBuilder cloud(CloudClient cloudClient) {
        this.storage = new CloudStorage(cloudClient);
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public OcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default {@link OcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public OcflStorageBuilder initializer(OcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * Configures whether inventory digests should be verified on read. This means computing the digest of the inventory
     * file and comparing it with the digest in the inventory's sidecar. Default: true.
     *
     * @param verifyInventoryDigest true if inventory digests should be verified on read
     * @return builder
     */
    public OcflStorageBuilder verifyInventoryDigest(boolean verifyInventoryDigest) {
        this.verifyInventoryDigest = verifyInventoryDigest;
        return this;
    }

    /**
     * Creates a {@link OcflStorage} object. One of {@link #storage(Storage)}, {@link #fileSystem(Path)}, or {@link #cloud(CloudClient)}
     * must be called before calling this method.
     *
     * @return a new {@link OcflStorage} object
     */
    public OcflStorage build() {
        Enforce.notNull(storage, "storage cannot be null");

        var init = initializer;
        if (init == null) {
            init = new DefaultOcflStorageInitializer(storage, objectMapper);
        }

        return new DefaultOcflStorage(storage, verifyInventoryDigest, init);
    }
}
