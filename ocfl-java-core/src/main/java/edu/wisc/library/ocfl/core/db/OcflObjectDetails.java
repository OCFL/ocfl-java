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

package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.model.RevisionId;

import java.time.LocalDateTime;

/**
 * Represents a ObjectDetails database record.
 */
public class OcflObjectDetails {

    private String objectId;
    private VersionId versionId;
    private RevisionId revisionId;
    private String objectRootPath;
    private String inventoryDigest;
    private DigestAlgorithm digestAlgorithm;
    private byte[] inventoryBytes;
    private LocalDateTime updateTimestamp;

    /**
     * The OCFL object id
     *
     * @return OCFL object id
     */
    public String getObjectId() {
        return objectId;
    }

    public OcflObjectDetails setObjectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    /**
     * The HEAD version of the object
     *
     * @return HEAD version of the object
     */
    public VersionId getVersionId() {
        return versionId;
    }

    public OcflObjectDetails setVersionId(VersionId versionId) {
        this.versionId = versionId;
        return this;
    }

    /**
     * The HEAD revision of the object. This will only be set if the mutable HEAD extension is used.
     *
     * @return HEAD revision id
     */
    public RevisionId getRevisionId() {
        return revisionId;
    }

    public OcflObjectDetails setRevisionId(RevisionId revisionId) {
        this.revisionId = revisionId;
        return this;
    }

    /**
     * The storage relative path to the object's root directory
     *
     * @return storate relative path to the object's root directory
     */
    public String getObjectRootPath() {
        return objectRootPath;
    }

    public OcflObjectDetails setObjectRootPath(String objectRootPath) {
        this.objectRootPath = objectRootPath;
        return this;
    }

    /**
     * The digest of the serialized inventory
     *
     * @return digest of the serialized inventory
     */
    public String getInventoryDigest() {
        return inventoryDigest;
    }

    public OcflObjectDetails setInventoryDigest(String inventoryDigest) {
        this.inventoryDigest = inventoryDigest;
        return this;
    }

    /**
     * The algorithm used to compute the inventory digest
     *
     * @return algorithm used to compute the inventory digest
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public OcflObjectDetails setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    /**
     * The bytes of the serialized inventory
     *
     * @return bytes of the serialized inventory
     */
    public byte[] getInventoryBytes() {
        return inventoryBytes;
    }

    public OcflObjectDetails setInventory(byte[] inventoryBytes) {
        this.inventoryBytes = inventoryBytes;
        return this;
    }

    /**
     * The timestamp the record was last updated
     *
     * @return timestamp the record was last updated
     */
    public LocalDateTime getUpdateTimestamp() {
        return updateTimestamp;
    }

    public OcflObjectDetails setUpdateTimestamp(LocalDateTime updateTimestamp) {
        this.updateTimestamp = updateTimestamp;
        return this;
    }

    @Override
    public String toString() {
        return "OcflObjectDetails{" +
                "objectId='" + objectId + '\'' +
                ", versionId=" + versionId +
                ", revisionId=" + revisionId +
                ", objectRootPath='" + objectRootPath + '\'' +
                ", inventoryDigest='" + inventoryDigest + '\'' +
                ", digestAlgorithm=" + digestAlgorithm +
                ", updateTimestamp=" + updateTimestamp +
                '}';
    }

}
