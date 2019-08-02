package edu.wisc.library.ocfl.api.model;

import java.util.Map;

public class ObjectDetails {

    private String id;
    private String headVersionId;
    private Map<String, VersionDetails> versions;

    public VersionDetails getHeadVersion() {
        return versions.get(headVersionId);
    }

    public String getId() {
        return id;
    }

    public ObjectDetails setId(String id) {
        this.id = id;
        return this;
    }

    public String getHeadVersionId() {
        return headVersionId;
    }

    public ObjectDetails setHeadVersionId(String headVersionId) {
        this.headVersionId = headVersionId;
        return this;
    }

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
