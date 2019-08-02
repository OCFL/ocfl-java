package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Objects;

public class ObjectId {

    public static final String HEAD = "HEAD";

    private final String objectId;
    private final String versionId;

    public static ObjectId head(String objectId) {
        return new ObjectId(objectId, HEAD);
    }

    public static ObjectId version(String objectId, String versionId) {
        return new ObjectId(objectId, versionId);
    }

    private ObjectId(String objectId, String versionId) {
        this.objectId = Enforce.notBlank(objectId, "objectId cannot be blank");
        this.versionId = Enforce.notBlank(versionId, "versionId cannot be blank");
    }

    public String getObjectId() {
        return objectId;
    }

    public String getVersionId() {
        return versionId;
    }

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
