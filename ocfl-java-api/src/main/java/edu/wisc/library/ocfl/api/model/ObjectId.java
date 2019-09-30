package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Objects;

/**
 * Points to a specific version of an object, encapsulating an object identifier and version identifier. If the versionId
 * is HEAD, then it points to whatever the most recent version of the object is.
 */
public class ObjectId {

    public static final String HEAD = "HEAD";

    private final String objectId;
    private final String versionId;

    /**
     * Creates an ObjectId instance that points to the HEAD version of the object
     *
     * @param objectId the id of the object
     */
    public static ObjectId head(String objectId) {
        return new ObjectId(objectId, HEAD);
    }

    /**
     * Creates an ObjectId instance that points to a specific version of an object
     *
     * @param objectId the id of the object
     * @param versionId the id of the version
     */
    public static ObjectId version(String objectId, String versionId) {
        return new ObjectId(objectId, versionId);
    }

    private ObjectId(String objectId, String versionId) {
        this.objectId = Enforce.notBlank(objectId, "objectId cannot be blank");
        this.versionId = Enforce.notBlank(versionId, "versionId cannot be blank");
        // TODO enforce version id format
    }

    /**
     * The object id
     */
    public String getObjectId() {
        return objectId;
    }

    /**
     * The version id
     */
    public String getVersionId() {
        return versionId;
    }

    /**
     * True if the versionId is HEAD
     */
    public boolean isHead() {
        return HEAD.equals(versionId);
    }

    @Override
    public String toString() {
        return "ObjectId{" +
                "objectId='" + objectId + '\'' +
                ", versionId='" + versionId + '\'' +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ObjectId that = (ObjectId) o;
        return objectId.equals(that.objectId) &&
                versionId.equals(that.versionId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(objectId, versionId);
    }

}
