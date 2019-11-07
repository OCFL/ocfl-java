package edu.wisc.library.ocfl.api.model;

import java.time.OffsetDateTime;
import java.util.Collection;

/**
 * Details about a specific version of an object
 */
public class VersionDetails {

    private String objectId;
    private VersionId versionId;
    private OffsetDateTime created;
    private CommitInfo commitInfo;
    private Collection<FileDetails> files;

    /**
     * The ObjectId of the version
     */
    public ObjectVersionId getObjectVersionId() {
        return ObjectVersionId.version(objectId, versionId);
    }

    /**
     * The object's id
     */
    public String getObjectId() {
        return objectId;
    }

    public VersionDetails setObjectId(String objectId) {
        this.objectId = objectId;
        return this;
    }

    /**
     * The version id
     */
    public VersionId getVersionId() {
        return versionId;
    }

    public VersionDetails setVersionId(VersionId versionId) {
        this.versionId = versionId;
        return this;
    }

    /**
     * The timestamp of when the version was created
     */
    public OffsetDateTime getCreated() {
        return created;
    }

    public VersionDetails setCreated(OffsetDateTime created) {
        this.created = created;
        return this;
    }

    /**
     * Optional description of the version
     */
    public CommitInfo getCommitInfo() {
        return commitInfo;
    }

    public VersionDetails setCommitInfo(CommitInfo commitInfo) {
        this.commitInfo = commitInfo;
        return this;
    }

    /**
     * Collection of all of the files in this version of the object
     */
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
