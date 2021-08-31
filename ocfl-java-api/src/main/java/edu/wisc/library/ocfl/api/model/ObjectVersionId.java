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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.wisc.library.ocfl.api.exception.InvalidVersionException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Objects;

/**
 * Points to a specific version of an object, encapsulating an object identifier and version number. When HEAD
 * is specified, then it points to whatever the most recent version of the object is.
 */
public class ObjectVersionId {

    private final String objectId;
    private final VersionNum versionNum;

    /**
     * Creates an ObjectId instance that points to the HEAD version of the object
     *
     * @param objectId the id of the object
     * @return new ObjectVersionId
     */
    public static ObjectVersionId head(String objectId) {
        return new ObjectVersionId(objectId);
    }

    /**
     * Creates an ObjectId instance that points to a specific version of an object
     *
     * @param objectId the id of the object
     * @param versionNum the OCFL version num of the version
     * @return new ObjectVersionId
     */
    public static ObjectVersionId version(String objectId, String versionNum) {
        if (versionNum == null) {
            return new ObjectVersionId(objectId, null);
        }
        return new ObjectVersionId(objectId, VersionNum.fromString(versionNum));
    }

    /**
     * Creates an ObjectId instance that points to a specific version of an object
     *
     * @param objectId the id of the object
     * @param versionNum the OCFL version num of the version
     * @return new ObjectVersionId
     */
    public static ObjectVersionId version(String objectId, int versionNum) {
        return new ObjectVersionId(objectId, VersionNum.fromInt(versionNum));
    }

    /**
     * Creates an ObjectId instance that points to a specific version of an object
     *
     * @param objectId the id of the object
     * @param versionNum the OCFL version number of the version
     * @return new ObjectVersionId
     */
    @JsonCreator
    public static ObjectVersionId version(@JsonProperty("objectId") String objectId,
                                          @JsonProperty("versionNum") VersionNum versionNum) {
        return new ObjectVersionId(objectId, versionNum);
    }

    private ObjectVersionId(String objectId) {
        this(objectId, null);
    }

    private ObjectVersionId(String objectId, VersionNum versionNum) {
        this.objectId = Enforce.notBlank(objectId, "objectId cannot be blank");
        this.versionNum = versionNum;
    }

    /**
     * The object id
     *
     * @return the object id
     */
    public String getObjectId() {
        return objectId;
    }

    /**
     * The version number
     *
     * @return the version number or null if no version is specified
     */
    public VersionNum getVersionNum() {
        return versionNum;
    }

    /**
     * @return true if no version number is set
     */
    @JsonIgnore
    public boolean isHead() {
        return versionNum == null;
    }

    /**
     * Returns a new ObjectVersionId instance with an incremented version number. This may only be called if this
     * instance has its version number set
     *
     * @return new ObjectVersionId with incremented version
     * @throws InvalidVersionException if the version number is not set or the version cannot be incremented
     */
    public ObjectVersionId nextVersion() {
        if (versionNum == null) {
            throw new InvalidVersionException("Cannot resolve next version number because the current version number is not set.");
        }
        return ObjectVersionId.version(objectId, versionNum.nextVersionNum());
    }

    /**
     * Returns a new ObjectVersionId instance with an decremented version number. This may only be called if this
     * instance has its version number set
     *
     * @return new ObjectVersionId with decremented version
     * @throws InvalidVersionException if the version number is not set or the version cannot be decremented
     */
    public ObjectVersionId previousVersion() {
        if (versionNum == null) {
            throw new InvalidVersionException("Cannot resolve previous version number because the current version number is not set.");
        }
        return ObjectVersionId.version(objectId, versionNum.previousVersionNum());
    }

    @Override
    public String toString() {
        return "ObjectId{" +
                "objectId='" + objectId + '\'' +
                ", versionNum='" + versionNum + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectVersionId that = (ObjectVersionId) o;
        return objectId.equals(that.objectId) &&
                versionNum.equals(that.versionNum);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, versionNum);
    }

}
