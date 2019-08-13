package edu.wisc.library.ocfl.api.model;

import java.time.OffsetDateTime;
import java.util.Collection;

public class VersionDetails {

    private String objectId;
    private String versionId;
    private OffsetDateTime created;
    private CommitInfo commitInfo;
    private Collection<FileDetails> files;

    public ObjectId getObjectVersionId() {
        return ObjectId.version(objectId, versionId);
    }

    public String getObjectId() {
        return objectId;
    }

    public VersionDetails setObjectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    public String getVersionId() {
        return versionId;
    }

    public VersionDetails setVersionId(String versionId) {
        this.versionId = versionId;
        return this;
    }

    public OffsetDateTime getCreated() {
        return created;
    }

    public VersionDetails setCreated(OffsetDateTime created) {
        this.created = created;
        return this;
    }

    public CommitInfo getCommitInfo() {
        return commitInfo;
    }

    public VersionDetails setCommitInfo(CommitInfo commitInfo) {
        this.commitInfo = commitInfo;
        return this;
    }

    public Collection<FileDetails> getFiles() {
        return files;
    }

    public VersionDetails setFiles(Collection<FileDetails> files) {
        this.files = files;
        return this;
    }

    @Override
    public String toString() {
        return "VersionDetails{" +
                "objectId='" + objectId + '\'' +
                ", versionId='" + versionId + '\'' +
                ", created=" + created +
                ", commitInfo=" + commitInfo +
                ", files=" + files +
                '}';
    }

}
