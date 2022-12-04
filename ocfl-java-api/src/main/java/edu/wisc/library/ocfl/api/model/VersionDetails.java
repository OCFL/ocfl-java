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

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Details about a specific version of an object
 */
public class VersionDetails {

    private ObjectVersionId objectVersionId;
    private OcflVersion objectOcflVersion;
    private OffsetDateTime created;
    private VersionInfo versionInfo;
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
    @JsonIgnore
    public String getObjectId() {
        return objectVersionId.getObjectId();
    }

    /**
     * The version number of this version
     *
     * @return the version number
     */
    @JsonIgnore
    public VersionNum getVersionNum() {
        return objectVersionId.getVersionNum();
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
     * @return VersionInfo
     */
    public VersionInfo getVersionInfo() {
        return versionInfo;
    }

    public VersionDetails setVersionInfo(VersionInfo versionInfo) {
        this.versionInfo = versionInfo;
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
     * @return the OCFL version the object adheres to
     */
    public OcflVersion getObjectOcflVersion() {
        return objectOcflVersion;
    }

    public VersionDetails setObjectOcflVersion(OcflVersion objectOcflVersion) {
        this.objectOcflVersion = objectOcflVersion;
        return this;
    }

    /**
     * Collection of all of the files in this version of the object
     *
     * @return all of the files in the version
     */
    @JsonIgnore
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
     * @return FileDetails or null
     */
    public FileDetails getFile(String path) {
        return fileMap.get(path);
    }

    public VersionDetails setFileMap(Map<String, FileDetails> fileMap) {
        this.fileMap = fileMap;
        return this;
    }

    /**
     * Returns a map of logical paths to file details that represents the state of the object at this version.
     *
     * @return file state map
     */
    public Map<String, FileDetails> getFileMap() {
        return fileMap;
    }

    @Override
    public String toString() {
        return "VersionDetails{" + "objectVersionId="
                + objectVersionId + ", objectOcflVersion="
                + objectOcflVersion + ", created="
                + created + ", versionInfo="
                + versionInfo + ", mutable="
                + mutable + ", fileMap="
                + fileMap + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        VersionDetails that = (VersionDetails) o;
        return mutable == that.mutable
                && Objects.equals(objectVersionId, that.objectVersionId)
                && objectOcflVersion == that.objectOcflVersion
                && Objects.equals(created, that.created)
                && Objects.equals(versionInfo, that.versionInfo)
                && Objects.equals(fileMap, that.fileMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectVersionId, objectOcflVersion, created, versionInfo, mutable, fileMap);
    }
}
