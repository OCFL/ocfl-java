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
    private VersionInfo versionInfo;

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
     * The object's version number for the version the changed occurred in
     *
     * @return version number
     */
    public VersionNum getVersionNum() {
        return objectVersionId.getVersionNum();
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
     * @return VersionInfo
     */
    public VersionInfo getVersionInfo() {
        return versionInfo;
    }

    public FileChange setVersionInfo(VersionInfo versionInfo) {
        this.versionInfo = versionInfo;
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
                ", versionInfo=" + versionInfo +
                '}';
    }

}
