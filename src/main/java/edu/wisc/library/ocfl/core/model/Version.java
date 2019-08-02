package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.*;

/**
 * OCFL version object. A Version describes the state of an object at a particular point in time.
 */
public class Version {

    private OffsetDateTime created;
    private String message;
    private User user;
    private Map<String, Set<String>> state;

    @JsonIgnore
    private Map<String, String> reverseStateMap;

    public Version() {
        state = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        reverseStateMap = new HashMap<>();
    }

    /**
     * The timestamp when this version of the object was created.
     */
    public OffsetDateTime getCreated() {
        return created;
    }

    public Version setCreated(OffsetDateTime created) {
        this.created = Enforce.notNull(created, "created cannot be null");
        return this;
    }

    /**
     * A human readable message describing the version.
     */
    public String getMessage() {
        return message;
    }

    public Version setMessage(String message) {
        this.message = message;
        return this;
    }

    /**
     * The person who created this version of the object.
     */
    public User getUser() {
        return user;
    }

    public Version setUser(User user) {
        this.user = user;
        return this;
    }

    /**
     * A map of all of the files that are part of this version of the object. The map is keyed on file digest ids, and the
     * values are paths that describe where the file is located in this specific version. The value is a set to conform
     * to the OCFL spec, but it will only ever contain a single file.
     */
    public Map<String, Set<String>> getState() {
        return state;
    }

    public Version setState(Map<String, Set<String>> state) {
        Enforce.notNull(state, "state cannot be null");
        this.state = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        this.state.putAll(state);
        this.state.forEach((digest, paths) -> paths.forEach(path -> reverseStateMap.put(path, digest)));
        return this;
    }

    public Map<String, Set<String>> cloneState() {
        var clone = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
        state.forEach((k, v) -> clone.put(k, new HashSet<>(v)));
        return clone;
    }

    /**
     * Helper method to add a file to a version. The path field should be relative to the content directory and not the
     * inventory root. See the OCFL spec for details.
     */
    public Version addFile(String id, String path) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(path, "path cannot be blank");

        state.computeIfAbsent(id, k -> new HashSet<>()).add(path);
        reverseStateMap.put(path, id);
        return this;
    }

    public void removePath(String path) {
        var id = reverseStateMap.remove(path);

        if (id != null) {
            var paths = state.get(id);
            if (paths.size() == 1) {
                state.remove(id);
            } else {
                paths.remove(path);
            }
        }
    }

    public String getFileId(String path) {
        return reverseStateMap.get(path);
    }

    public Set<String> getPaths(String id) {
        return state.get(id);
    }

    public Set<String> listPaths() {
        return reverseStateMap.keySet();
    }

    @Override

    public String toString() {
        return "Version{" +
                "created=" + created +
                ", message='" + message + '\'' +
                ", user=" + user +
                ", state=" + state +
                '}';
    }

}
