package edu.wisc.library.ocfl.api.model;

import java.time.OffsetDateTime;
import java.util.Map;

/**
 * Details about a change to a file.
 */
public class FileChange {

    private FileChangeType changeType;
    private ObjectVersionId objectVersionId;
    private String path;
    private String storageRelativePath;
    private Map<DigestAlgorithm, String> fixity;
    private OffsetDateTime timestamp;
    private CommitInfo commitInfo;

    /**
     * The type of change that occurred, UPDATE/REMOVE.
     *
     * @return type of change
     */
    public FileChangeType getChangeType() {
        return changeType;
    }

    public FileChange setChangeType(FileChangeType changeType) {
        this.changeType = changeType;
        return this;
    }

    /**
     * The ObjectVersionId for the version the changed occurred in
     *
     * @return ObjectVersionId
     */
    public ObjectVersionId getObjectVersionId() {
        return objectVersionId;
    }

    public FileChange setObjectVersionId(ObjectVersionId objectVersionId) {
        this.objectVersionId = objectVersionId;
        return this;
    }

    /**
     * The object's version id for the version the changed occurred in
     *
     * @return VersionId
     */
    public VersionId getVersionId() {
        return objectVersionId.getVersionId();
    }

    /**
     * The file's logical path
     *
     * @return logical path
     */
    public String getPath() {
        return path;
    }

    public FileChange setPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * The file's path relative to the storage root. Null on {@link FileChangeType#REMOVE}.
     *
     * @return storage relative path or null
     */
    public String getStorageRelativePath() {
        return storageRelativePath;
    }

    public FileChange setStorageRelativePath(String storageRelativePath) {
        this.storageRelativePath = storageRelativePath;
        return this;
    }

    /**
     * Map of digest algorithm to digest value. Empty on {@link FileChangeType#REMOVE}.
     *
     * @return digest map
     */
    public Map<DigestAlgorithm, String> getFixity() {
        return fixity;
    }

    public FileChange setFixity(Map<DigestAlgorithm, String> fixity) {
        this.fixity = fixity;
        return this;
    }

    /**
     * The timestamp when the file changed
     *
     * @return timestamp
     */
    public OffsetDateTime getTimestamp() {
        return timestamp;
    }

    public FileChange setTimestamp(OffsetDateTime timestamp) {
        this.timestamp = timestamp;
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

    public FileChange setCommitInfo(CommitInfo commitInfo) {
        this.commitInfo = commitInfo;
        return this;
    }

    @Override
    public String toString() {
        return "FileChange{" +
                "objectVersionId=" + objectVersionId +
                ", changeType=" + changeType +
                ", path='" + path + '\'' +
                ", storageRelativePath='" + storageRelativePath + '\'' +
                ", fixity=" + fixity +
                ", timestamp=" + timestamp +
                ", commitInfo=" + commitInfo +
                '}';
    }

}
