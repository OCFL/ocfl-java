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

package io.ocfl.api.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Details the current state of an object and all of its versions.
 */
public class ObjectDetails {

    private String id;
    private VersionNum headVersionNum;
    private DigestAlgorithm digestAlgorithm;
    private OcflVersion objectOcflVersion;
    private Map<VersionNum, VersionDetails> versions;

    public ObjectDetails() {
        versions = new HashMap<>();
    }

    /**
     * The version details of the HEAD version of the object
     *
     * @return VersionDetails for the object's HEAD version
     */
    @JsonIgnore
    public VersionDetails getHeadVersion() {
        return versions.get(headVersionNum);
    }

    /**
     * @return the object's id
     */
    public String getId() {
        return id;
    }

    public ObjectDetails setId(String id) {
        this.id = id;
        return this;
    }

    /**
     * The version number of the HEAD version of the object
     *
     * @return the version number of the object's HEAD version
     */
    public VersionNum getHeadVersionNum() {
        return headVersionNum;
    }

    public ObjectDetails setHeadVersionNum(VersionNum headVersionNum) {
        this.headVersionNum = headVersionNum;
        return this;
    }

    /**
     * @return the digest algorithm used to identify files within the OCFL object
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public ObjectDetails setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
        return this;
    }

    /**
     * @return the OCFL version the object adheres to
     */
    public OcflVersion getObjectOcflVersion() {
        return objectOcflVersion;
    }

    public ObjectDetails setObjectOcflVersion(OcflVersion objectOcflVersion) {
        this.objectOcflVersion = objectOcflVersion;
        return this;
    }

    /**
     * Map of version number to version details for all of the versions of the object.
     *
     * @return map of all of the object's versions
     */
    public Map<VersionNum, VersionDetails> getVersionMap() {
        return versions;
    }

    /**
     * Returns the VersionDetails for the specified version number or null if the version does not exist
     *
     * @param versionNum the version number of the version to retrieve
     * @return version details or null
     */
    public VersionDetails getVersion(VersionNum versionNum) {
        return versions.get(versionNum);
    }

    public ObjectDetails setVersions(Map<VersionNum, VersionDetails> versions) {
        this.versions = versions;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectDetails{" + "id='"
                + id + '\'' + ", headVersionNum="
                + headVersionNum + ", digestAlgorithm="
                + digestAlgorithm + ", objectOcflVersion="
                + objectOcflVersion + ", versions="
                + versions + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        ObjectDetails that = (ObjectDetails) o;
        return Objects.equals(id, that.id)
                && Objects.equals(headVersionNum, that.headVersionNum)
                && Objects.equals(digestAlgorithm, that.digestAlgorithm)
                && objectOcflVersion == that.objectOcflVersion
                && Objects.equals(versions, that.versions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, headVersionNum, digestAlgorithm, objectOcflVersion, versions);
    }
}
