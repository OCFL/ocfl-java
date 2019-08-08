package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.*;

public class VersionBuilder {

    private OffsetDateTime created;
    private String message;
    private User user;
    private Map<String, Set<String>> state;
    private Map<String, String> reverseStateMap;

    public VersionBuilder() {
        state = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        reverseStateMap = new HashMap<>();
    }

    public VersionBuilder(Version original) {
        Enforce.notNull(original, "version cannot be null");
        state = original.getMutableState();
        reverseStateMap = original.getMutableReverseStateMap();
    }

    public String getFileId(String path) {
        return reverseStateMap.get(path);
    }

    public VersionBuilder removePath(String path) {
        var id = reverseStateMap.remove(path);

        if (id != null) {
            var paths = state.get(id);
            if (paths.size() == 1) {
                state.remove(id);
            } else {
                paths.remove(path);
            }
        }

        return this;
    }

    /**
     * The path field should be relative to the content directory and not the inventory root.
     */
    public VersionBuilder addFile(String id, String path) {
        Enforce.notBlank(id, "id cannot be blank");
        Enforce.notBlank(path, "path cannot be blank");

        state.computeIfAbsent(id, k -> new HashSet<>()).add(path);
        reverseStateMap.put(path, id);
        return this;
    }

    public VersionBuilder created(OffsetDateTime created) {
        this.created = created;
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
        this.message = commitInfo.getMessage();
        if (commitInfo.getUser() != null) {
            this.user = new User(commitInfo.getUser().getName(), commitInfo.getUser().getAddress());
        }
        return this;
    }

    public VersionBuilder state(Map<String, Set<String>> state) {
        this.state = state;
        return this;
    }

    public VersionBuilder reverseStateMap(Map<String, String> reverseStateMap) {
        this.reverseStateMap = reverseStateMap;
        return this;
    }

    public Version build() {
        return new Version(created, message, user, state, reverseStateMap);
    }

}
