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

import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Map;

/**
 * View of a specific version of an OCFL object that allows its files to be lazy-loaded.
 */
public class OcflObjectVersion {

    private final VersionDetails versionDetails;
    private final Map<String, OcflObjectVersionFile> fileMap;

    public OcflObjectVersion(VersionDetails versionDetails, Map<String, OcflObjectVersionFile> fileMap) {
        this.versionDetails = Enforce.notNull(versionDetails, "versionDetails cannot be null");
        this.fileMap = Enforce.notNull(fileMap, "fileMap cannot be null");
    }

    /**
     * The ObjectId of the version
     *
     * @return the ObjectVersionId of the version
     */
    public ObjectVersionId getObjectVersionId() {
        return versionDetails.getObjectVersionId();
    }

    /**
     * The object's id
     *
     * @return the object's id
     */
    public String getObjectId() {
        return versionDetails.getObjectId();
    }

    /**
     * The version id
     *
     * @return the VersionId
     */
    public VersionId getVersionId() {
        return versionDetails.getVersionId();
    }

    /**
     * The timestamp of when the version was created
     *
     * @return created timestamp
     */
    public OffsetDateTime getCreated() {
        return versionDetails.getCreated();
    }

    /**
     * Optional description of the version
     *
     * @return VersionInfo or null
     */
    public VersionInfo getVersionInfo() {
        return versionDetails.getVersionInfo();
    }

    /**
     * Returns true only if the version is a mutable HEAD version that is used to stage changes.
     *
     * @return true if mutable HEAD
     */
    public boolean isMutable() {
        return versionDetails.isMutable();
    }

    /**
     * Collection of all of the files in this version of the object
     *
     * @return all of the files in the version
     */
    public Collection<OcflObjectVersionFile> getFiles() {
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
     * Returns the OcflObjectVersionFile for the file at the given path or null if it does not exist
     *
     * @param path logical path to the file
     * @return OcflObjectVersionFile or null if it does not exist
     */
    public OcflObjectVersionFile getFile(String path) {
        return fileMap.get(path);
    }

    @Override
    public String toString() {
        return "OcflObjectVersion{" +
                "versionDetails='" + versionDetails + '\'' +
                ", fileMap=" + fileMap +
                '}';
    }

}
