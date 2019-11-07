package edu.wisc.library.ocfl.api.model;

import java.util.HashMap;
import java.util.Map;

/**
 * Details the current state of an object and all of its versions.
 */
public class ObjectDetails {

    private String id;
    private VersionId headVersionId;
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
                ", versions=" + versions +
                '}';
    }

}
