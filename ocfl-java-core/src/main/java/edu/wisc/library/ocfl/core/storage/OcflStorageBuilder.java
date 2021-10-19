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

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.storage.cloud.CloudClient;
import edu.wisc.library.ocfl.core.storage.cloud.CloudStorage;
import edu.wisc.library.ocfl.core.storage.common.Storage;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemStorage;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.nio.file.Path;

// TODO docs
/**
 * Builder for constructing S3OcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new S3OcflStorageBuilder().s3Client(s3Client).workDir(workDir).build(bucketName).}
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

    // TODO
    public OcflStorageBuilder storage(Storage storage) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        return this;
    }

    public OcflStorageBuilder fileSystem(Path storageRoot) {
        this.storage = new FileSystemStorage(storageRoot);
        return this;
    }

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
