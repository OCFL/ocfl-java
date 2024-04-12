/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.model;

import io.ocfl.api.exception.OcflInputException;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.api.util.Enforce;
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

    public VersionBuilder versionInfo(VersionInfo versionInfo) {
        if (versionInfo != null) {
            this.message = versionInfo.getMessage();
            if (versionInfo.getUser() != null && versionInfo.getUser().getName() != null) {
                this.user = new User(
                        versionInfo.getUser().getName(), versionInfo.getUser().getAddress());
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
     * Validates that the logical path does not conflict with any existing logical paths in this version. Paths conflict
     * if one expects a path to be a directory and another expects it to be a file.
     *
     * @param logicalPath the logical path
     * @throws OcflInputException if there is a conflict
     */
    public void validateNonConflictingPath(String logicalPath) {
        var pathAsDir = logicalPath + "/";
        state.getPathToFileId().keySet().forEach(existingPath -> {
            if (existingPath.startsWith(pathAsDir)) {
                throw conflictException(logicalPath, existingPath);
            }

            var existingAsDir = existingPath + "/";
            if (logicalPath.startsWith(existingAsDir)) {
                throw conflictException(logicalPath, existingPath);
            }
        });
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

    private OcflInputException conflictException(String logicalPath, String existingPath) {
        return new OcflInputException(
                String.format("The logical path %s conflicts with the existing path %s.", logicalPath, existingPath));
    }
}
