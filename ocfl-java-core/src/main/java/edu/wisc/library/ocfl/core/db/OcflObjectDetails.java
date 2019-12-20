package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.model.RevisionId;

import java.time.LocalDateTime;

public class OcflObjectDetails {

    private String objectId;
    private VersionId versionId;
    private RevisionId revisionId;
    private String objectRootPath;
    private String inventoryDigest;
    private DigestAlgorithm digestAlgorithm;
    private byte[] inventoryBytes;
    private LocalDateTime updateTimestamp;

    public String getObjectId() {
        return objectId;
    }

    public OcflObjectDetails setObjectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public VersionId getVersionId() {
        return versionId;
    }

    public OcflObjectDetails setVersionId(VersionId versionId) {
        this.versionId = versionId;
        return this;
    }

    public RevisionId getRevisionId() {
        return revisionId;
    }

    public OcflObjectDetails setRevisionId(RevisionId revisionId) {
        this.revisionId = revisionId;
        return this;
    }

    public String getObjectRootPath() {
        return objectRootPath;
    }

    public OcflObjectDetails setObjectRootPath(String objectRootPath) {
        this.objectRootPath = objectRootPath;
        return this;
    }

    public String getInventoryDigest() {
        return inventoryDigest;
    }

    public OcflObjectDetails setInventoryDigest(String inventoryDigest) {
        this.inventoryDigest = inventoryDigest;
        return this;
    }

    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public OcflObjectDetails setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    public byte[] getInventoryBytes() {
        return inventoryBytes;
    }

    public OcflObjectDetails setInventory(byte[] inventoryBytes) {
        this.inventoryBytes = inventoryBytes;
        return this;
    }

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
