package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

/**
 * Used to construct Version objects.
 */
public class VersionBuilder {

    private OffsetDateTime created;
    private String message;
    private User user;
    private PathBiMap state;

    public VersionBuilder() {
        state = new PathBiMap();
    }

    /**
     * Used to construct a new Version that's based on an existing version. The existing version's state is copied over
     * to the new version.
     *
     * @param original the original version
     */
    public VersionBuilder(Version original) {
        Enforce.notNull(original, "version cannot be null");
        state = PathBiMap.fromFileIdMap(original.getState());
    }

    /**
     * Adds a file to the version's state
     *
     * @param id the fileId
     * @param logicalPath the logical path to the file
     * @return builder
     */
    public VersionBuilder addFile(String id, String logicalPath) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(logicalPath, "logicalPath cannot be blank");

        state.put(id, logicalPath);
        return this;
    }

    public VersionBuilder created(OffsetDateTime created) {
        this.created = Enforce.notNull(created, "created cannot be null");
        return this;
    }

    public VersionBuilder message(String message) {
        this.message = message;
        return this;
    }

    public VersionBuilder user(User user) {
        this.user = user;
        return this;
    }

    public VersionBuilder commitInfo(CommitInfo commitInfo) {
        if (commitInfo != null) {
            this.message = commitInfo.getMessage();
            if (commitInfo.getUser() != null) {
                this.user = new User(commitInfo.getUser().getName(), commitInfo.getUser().getAddress());
            }
        }
        return this;
    }

    public VersionBuilder state(Map<String, Set<String>> state) {
        this.state = PathBiMap.fromFileIdMap(Enforce.notNull(state, "state cannot be null"));
        return this;
    }

    public VersionBuilder state(PathBiMap state) {
        this.state = Enforce.notNull(state, "state cannot be null");
        return this;
    }

    /**
     * @return a new Version
     */
    public Version build() {
        return new Version(created, message, user, state.getFileIdToPaths());
    }

    /**
     * Indicates if the state contains the fileId
     *
     * @param fileId the fileId
     * @return true if the state contains the fileId
     */
    public boolean containsFileId(String fileId) {
        return state.containsFileId(fileId);
    }

    /**
     * Indicates if the state contains the logicalPath
     *
     * @param logicalPath the logicalPath
     * @return true if the state contains the logicalPath
     */
    public boolean containsLogicalPath(String logicalPath) {
        return state.containsPath(logicalPath);
    }

    /**
     * Retrieves all of the logical paths associated to the fileId or an empty set
     *
     * @param fileId the fileId
     * @return associated logical paths or an empty set
     */
    public Set<String> getLogicalPaths(String fileId) {
        return state.getPaths(fileId);
    }

    /**
     * Retrieves the fileId associated to the logicalPath
     *
     * @param logicalPath the logicalPath
     * @return the fileId or null
     */
    public String getFileId(String logicalPath) {
        return state.getFileId(logicalPath);
    }

    /**
     * Removes a logical path from the state
     *
     * @param logicalPath the logicalPath
     * @return the fileId associated to the path or null
     */
    public String removeLogicalPath(String logicalPath) {
        return state.removePath(logicalPath);
    }

    /**
     * Removes a fileId from the state
     *
     * @param fileId the fileId
     * @return the logical paths associated to the fileId
     */
    public Set<String> removeFileId(String fileId) {
        return state.removeFileId(fileId);
    }

    /**
     * Returns a map of logical paths to file ids of all of the files in the version's state
     *
     * @return map of logical paths to file ids
     */
    public Map<String, String> getInvertedState() {
        return state.getPathToFileId();
    }

}
