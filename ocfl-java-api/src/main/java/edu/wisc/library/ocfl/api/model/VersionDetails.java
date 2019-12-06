package edu.wisc.library.ocfl.api.model;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

/**
 * Details about a specific version of an object
 */
public class VersionDetails {

    private ObjectVersionId objectVersionId;
    private OffsetDateTime created;
    private CommitInfo commitInfo;
    private boolean mutable;
    private Map<String, FileDetails> fileMap;

    public VersionDetails() {
        fileMap = new HashMap<>();
    }

    /**
     * The ObjectVersionId of the version
     *
     * @return the ObjectVersionId of the version
     */
    public ObjectVersionId getObjectVersionId() {
        return objectVersionId;
    }

    public VersionDetails setObjectVersionId(ObjectVersionId objectVersionId) {
        this.objectVersionId = objectVersionId;
        return this;
    }

    /**
     * The object's id
     *
     * @return the object's id
     */
    public String getObjectId() {
        return objectVersionId.getObjectId();
    }

    /**
     * The version id
     *
     * @return the VersionId
     */
    public VersionId getVersionId() {
        return objectVersionId.getVersionId();
    }

    /**
     * The timestamp of when the version was created
     *
     * @return created timestamp
     */
    public OffsetDateTime getCreated() {
        return created;
    }

    public VersionDetails setCreated(OffsetDateTime created) {
        this.created = created;
        return this;
    }

    /**
     * Description of the version
     *
     * @return CommitInfo
     */
    public CommitInfo getCommitInfo() {
        return commitInfo;
    }

    public VersionDetails setCommitInfo(CommitInfo commitInfo) {
        this.commitInfo = commitInfo;
        return this;
    }

    /**
     * Returns true only if the version is a mutable HEAD version that is used to stage changes.
     *
     * @return true if mutable HEAD
     */
    public boolean isMutable() {
        return mutable;
    }

    public VersionDetails setMutable(boolean mutable) {
        this.mutable = mutable;
        return this;
    }

    /**
     * Collection of all of the files in this version of the object
     *
     * @return all of the files in the version
     */
    public Collection<FileDetails> getFiles() {
        return fileMap.values();
    }

    /**
     * Returns true if the version contains a file at the specified path
     *
     * @param path logical path to an object file
     * @return true if the version contains the file
     */
    public boolean containsFile(String path) {
        return fileMap.containsKey(path);
    }

    /**
     * Returns the FileDetails for the file at the given path or null if it does not exist
     *
     * @param path logical path to the file
     * @return FileDetails
     */
    public FileDetails getFile(String path) {
        return fileMap.get(path);
    }

    public VersionDetails setFileMap(Map<String, FileDetails> fileMap) {
        this.fileMap = fileMap;
        return this;
    }

    @Override
    public String toString() {
        return "VersionDetails{" +
                "objectVersionId='" + objectVersionId + '\'' +
                ", created=" + created +
                ", commitInfo=" + commitInfo +
                ", mutable=" + mutable +
                ", fileMap=" + fileMap +
                '}';
    }

}
