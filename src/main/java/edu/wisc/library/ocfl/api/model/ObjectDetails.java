package edu.wisc.library.ocfl.api.model;

import java.util.Map;

/**
 * Details the current state of an object and all of its versions.
 */
public class ObjectDetails {

    private String id;
    private String headVersionId;
    private Map<String, VersionDetails> versions;

    /**
     * The version details of the head version of the object
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
     * The version id of the head version of the object
     */
    public String getHeadVersionId() {
        return headVersionId;
    }

    public ObjectDetails setHeadVersionId(String headVersionId) {
        this.headVersionId = headVersionId;
        return this;
    }

    /**
     * Map of version id to version details for all of the versions of the object.
     */
    public Map<String, VersionDetails> getVersions() {
        return versions;
    }

    public ObjectDetails setVersions(Map<String, VersionDetails> versions) {
        this.versions = versions;
        return this;
    }

    @Override
    public String toString() {
        return "ObjectDetails{" +
                "id='" + id + '\'' +
                ", headVersionId='" + headVersionId + '\'' +
                ", versions=" + versions +
                '}';
    }

}
