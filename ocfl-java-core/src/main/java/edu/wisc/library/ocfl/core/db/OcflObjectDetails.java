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
