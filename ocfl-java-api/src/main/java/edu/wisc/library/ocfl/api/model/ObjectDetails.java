package edu.wisc.library.ocfl.api.model;

import java.util.Map;

/**
 * Details the current state of an object and all of its versions.
 */
public class ObjectDetails {

    private String id;
    private VersionId headVersionId;
    private Map<VersionId, VersionDetails> versions;

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
    public VersionId getHeadVersionId() {
        return headVersionId;
    }

    public ObjectDetails setHeadVersionId(VersionId headVersionId) {
        this.headVersionId = headVersionId;
        return this;
    }

    /**
     * Map of version id to version details for all of the versions of the object.
     */
    public Map<VersionId, VersionDetails> getVersions() {
        return versions;
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
                ", versions=" + versions +
                '}';
    }

}
