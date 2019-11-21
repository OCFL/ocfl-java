package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.*;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Set;

/**
 * OCFL version object. A Version describes the state of an object at a particular point in time. Versions are immutable.
 *
 * @see VersionBuilder
 */
@JsonPropertyOrder({
        "created",
        "message",
        "user",
        "state"
})
public class Version {

    private final OffsetDateTime created;
    private final String message;
    private final User user;

    @JsonIgnore
    private final PathBiMap stateBiMap;

    public static VersionBuilder builder() {
        return new VersionBuilder();
    }

    public static VersionBuilder builder(Version original) {
        return new VersionBuilder(original);
    }

    /**
     * @see VersionBuilder
     */
    @JsonCreator
    public Version(
            @JsonProperty("created") OffsetDateTime created,
            @JsonProperty("message") String message,
            @JsonProperty("user") User user,
            @JsonProperty("state") Map<String, Set<String>> state) {
        this.created = Enforce.notNull(created, "created cannot be null");
        this.message = message;
        this.user = user;
        this.stateBiMap = PathBiMap.fromFileIdMap(state);
    }

    /**
     * The timestamp when this version of the object was created.
     */
    @JsonGetter("created")
    public OffsetDateTime getCreated() {
        return created;
    }

    /**
     * A human readable message describing the version.
     */
    @JsonGetter("message")
    public String getMessage() {
        return message;
    }

    /**
     * The person who created this version of the object.
     */
    @JsonGetter("user")
    public User getUser() {
        return user;
    }

    /**
     * A map of all of the files that are part of this version of the object. The map is keyed on file digest ids, and the
     * values are paths that describe where the file is located in this specific version.
     */
    @JsonGetter("state")
    public Map<String, Set<String>> getState() {
        return stateBiMap.getFileIdToPaths();
    }

    public String getFileId(String path) {
        return stateBiMap.getFileId(path);
    }

    public Set<String> getPaths(String fileId) {
        return stateBiMap.getPaths(fileId);
    }

    @Override
    public String toString() {
        return "Version{" +
                "created=" + created +
                ", message='" + message + '\'' +
                ", user=" + user +
                ", state=" + stateBiMap +
                '}';
    }

}
