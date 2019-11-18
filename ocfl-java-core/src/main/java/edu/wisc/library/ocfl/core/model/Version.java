package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Function;

/**
 * OCFL version object. A Version describes the state of an object at a particular point in time. Versions are immutable.
 *
 * @see VersionBuilder
 */
public class Version {

    private final OffsetDateTime created;
    private final String message;
    private final User user;
    private final Map<String, Set<String>> state;

    @JsonIgnore
    private final Map<String, String> reverseStateMap;

    /**
     * This constructor is used by Jackson for deserialization.
     */
    @JsonCreator
    public Version(
            @JsonProperty("created") OffsetDateTime created,
            @JsonProperty("message") String message,
            @JsonProperty("user") User user,
            @JsonProperty("state") Map<String, Set<String>> state) {
        this(created, message, user, state, null);
    }

    public static VersionBuilder builder() {
        return new VersionBuilder();
    }

    /**
     * @see VersionBuilder
     */
    public Version(
            OffsetDateTime created,
            String message,
            User user,
            Map<String, Set<String>> state,
            Map<String, String> reverseStateMap) {
        this.created = Enforce.notNull(created, "created cannot be null");
        this.message = message;
        this.user = user;
        this.state = Collections.unmodifiableMap(copyState(state, Collections::unmodifiableSet));
        if (reverseStateMap == null) {
            this.reverseStateMap = Collections.unmodifiableMap(createReverseStateMap(this.state));
        } else {
            this.reverseStateMap = Map.copyOf(reverseStateMap);
        }
    }

    private Map<String, Set<String>> copyState(Map<String, Set<String>> state, Function<Set<String>, Set<String>> pathSetCreator) {
        Enforce.notNull(state, "state cannot be null");
        var newState = new TreeMap<String, Set<String>>(String.CASE_INSENSITIVE_ORDER);
        state.forEach((digest, paths) -> newState.put(digest, pathSetCreator.apply(new TreeSet<>(paths))));
        return newState;
    }

    private Map<String, String> createReverseStateMap(Map<String, Set<String>> state) {
        var reverseMap = new HashMap<String, String>();
        state.forEach((digest, paths) -> paths.forEach(path -> reverseMap.put(path, digest)));
        return reverseMap;
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
        return state;
    }

    @JsonIgnore
    public Map<String, Set<String>> getMutableState() {
        return copyState(state, Function.identity());
    }

    @JsonIgnore
    public Map<String, String> getMutableReverseStateMap() {
        return new HashMap<>(reverseStateMap);
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
