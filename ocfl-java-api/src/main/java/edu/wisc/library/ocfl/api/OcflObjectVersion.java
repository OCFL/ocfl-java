package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Collection;
import java.util.Map;

/**
 * View of a specific version of an OCFL object that allows its files to be lazy-loaded.
 */
public class OcflObjectVersion {

    private VersionDetails versionDetails;
    private Map<String, OcflObjectVersionFile> fileMap;

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
     * @return CommitInfo or null
     */
    public CommitInfo getCommitInfo() {
        return versionDetails.getCommitInfo();
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
     * @return OcflObjectVersionFile
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
