/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.time.OffsetDateTime;
import java.util.Map;
import java.util.Objects;
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
     *
     * @param created timestamp the version was created
     * @param message version message
     * @param user user who created the version
     * @param state version state
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
     * @return the timestamp when this version of the object was created.
     */
    @JsonGetter("created")
    public OffsetDateTime getCreated() {
        return created;
    }

    /**
     * @return a human readable message describing the version.
     */
    @JsonGetter("message")
    public String getMessage() {
        return message;
    }

    /**
     * @return the person who created this version of the object.
     */
    @JsonGetter("user")
    public User getUser() {
        return user;
    }

    /**
     * A map of all of the files that are part of this version of the object. The map is keyed on file digest ids, and the
     * values are paths that describe where the file is located in this specific version.
     *
     * @return version state
     */
    @JsonGetter("state")
    public Map<String, Set<String>> getState() {
        return stateBiMap.getFileIdToPaths();
    }

    /**
     * Returns the file id associated to the given logical path.
     *
     * @param path logical path
     * @return associated file id
     */
    public String getFileId(String path) {
        return stateBiMap.getFileId(path);
    }

    /**
     * Returns the logical paths associated with the given file id
     *
     * @param fileId file id
     * @return associated logical paths
     */
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Version version = (Version) o;
        return created.equals(version.created) &&
                Objects.equals(message, version.message) &&
                Objects.equals(user, version.user) &&
                stateBiMap.equals(version.stateBiMap);
    }

    @Override
    public int hashCode() {
        return Objects.hash(created, message, user, stateBiMap);
    }

}
