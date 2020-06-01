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

import java.util.HashMap;
import java.util.Map;

/**
 * Details the current state of an object and all of its versions.
 */
public class ObjectDetails {

    private String id;
    private VersionId headVersionId;
    private DigestAlgorithm digestAlgorithm;
    private Map<VersionId, VersionDetails> versions;

    public ObjectDetails() {
        versions = new HashMap<>();
    }

    /**
     * The version details of the HEAD version of the object
     *
     * @return VersionDetails for the object's HEAD version
     */
    public VersionDetails getHeadVersion() {
        return versions.get(headVersionId);
    }

    /**
     * The object's id
     */
    public String getId() {
        return id;
    }

    public ObjectDetails setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * The version id of the HEAD version of the object
     *
     * @return the version id of the object's HEAD version
     */
    public VersionId getHeadVersionId() {
        return headVersionId;
    }

    public ObjectDetails setHeadVersionId(VersionId headVersionId) {
        this.headVersionId = headVersionId;
        return this;
    }

    /**
     * The digest algorithm used to identify files within the OCFL object
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public ObjectDetails setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    /**
     * Map of version id to version details for all of the versions of the object.
     *
     * @return map of all of the object's versions
     */
    public Map<VersionId, VersionDetails> getVersionMap() {
        return versions;
    }

    /**
     * Returns the VersionDetails for the specified VersionId or null if the version does not exist
     *
     * @param versionId the version id of the version to retrieve
     * @return version details
     */
    public VersionDetails getVersion(VersionId versionId) {
        return versions.get(versionId);
    }

    public ObjectDetails setVersions(Map<VersionId, VersionDetails> versions) {
        this.versions = versions;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectDetails{" +
                "id='" + id + '\'' +
                ", headVersionId='" + headVersionId + '\'' +
                ", digestAlgorithm='" + digestAlgorithm + '\'' +
                ", versions=" + versions +
                '}';
    }

}
