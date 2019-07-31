package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * OCFL version object. A Version describes the state of an object at a particular point in time.
 */
public class Version {

    private OffsetDateTime created;
    private String message;
    private User user;
    private Map<String, Set<String>> state;

    public Version() {
        state = new HashMap<>();
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
        this.state = Enforce.notNull(state, "state cannot be null");
        return this;
    }

    /**
     * Helper method to add a file to a version. The path field should be relative to the content directory and not the
     * inventory root. See the OCFL spec for details.
     */
    public Version addFile(String id, String path) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(path, "path cannot be blank");

        state.computeIfAbsent(id, k -> new HashSet<>()).add(path);
        return this;
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
