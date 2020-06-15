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

package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.ArrayList;
import java.util.List;

/**
 * A PathConstraintProcessor is used to apply a preconfigured set of constraints to a given path. If the path fails to
 * meet a constraint, a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException} is thrown.
 */
public class PathConstraintProcessor {

    private final List<PathConstraint> pathConstraints;
    private final List<FileNameConstraint> fileNameConstraints;
    private final List<PathCharConstraint> charConstraints;

    /**
     * Use this to construct a new PathConstraintProcessor
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Use this to construct a new PathConstraintProcessor
     */
    public static class Builder {

        private List<PathConstraint> pathConstraints = new ArrayList<>();
        private List<FileNameConstraint> fileNameConstraints = new ArrayList<>();
        private List<PathCharConstraint> charConstraints = new ArrayList<>();

        /**
         * Adds a new path constraint
         *
         * @param pathConstraint constraint to add
         * @return builder
         */
        public Builder pathConstraint(PathConstraint pathConstraint) {
            pathConstraints.add(Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
            return this;
        }

        /**
         * Replaces the list of path constraints
         *
         * @param pathConstraints list of path constraints to use
         * @return builder
         */
        public Builder pathConstraints(List<PathConstraint> pathConstraints) {
            this.pathConstraints = Enforce.notNull(pathConstraints, "pathConstraints cannot be null");
            return this;
        }

        /**
         * Adds a new filename constraint
         *
         * @param fileNameConstraint constraint to add
         * @return builder
         */
        public Builder fileNameConstraint(FileNameConstraint fileNameConstraint) {
            fileNameConstraints.add(Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
            return this;
        }

        /**
         * Replaces the list of filename constraints
         *
         * @param fileNameConstraints list of filename constraints to use
         * @return builder
         */
        public Builder fileNameConstraints(List<FileNameConstraint> fileNameConstraints) {
            this.fileNameConstraints = Enforce.notNull(fileNameConstraints, "fileNameConstraints cannot be null");
            return this;
        }

        /**
         * Adds a new character constraint
         *
         * @param charConstraint constraint to add
         * @return builder
         */
        public Builder charConstraint(PathCharConstraint charConstraint) {
            charConstraints.add(Enforce.notNull(charConstraint, "charConstraint cannot be null"));
            return this;
        }

        /**
         * Replaces the list of character constraints
         *
         * @param charConstraints list of character constraints to use
         * @return builder
         */
        public Builder charConstraints(List<PathCharConstraint> charConstraints) {
            this.charConstraints = Enforce.notNull(charConstraints, "charConstraints cannot be null");
            return this;
        }

        /**
         * @return new PathConstraintProcessor
         */
        public PathConstraintProcessor build() {
            return new PathConstraintProcessor(pathConstraints, fileNameConstraints, charConstraints);
        }

    }

    /**
     * Constructs a new PathConstraintProcessor. The builder {@link PathConstraintProcessor.Builder} may also be used.
     *
     * @param pathConstraints list of path constraints
     * @param fileNameConstraints list of filename constraints
     * @param charConstraints list of character constraints
     */
    public PathConstraintProcessor(List<PathConstraint> pathConstraints, List<FileNameConstraint> fileNameConstraints, List<PathCharConstraint> charConstraints) {
        this.pathConstraints = Enforce.notNull(pathConstraints, "pathConstraints cannot be null");
        this.fileNameConstraints = Enforce.notNull(fileNameConstraints, "fileNameConstraints cannot be null");
        this.charConstraints = Enforce.notNull(charConstraints, "charConstraints cannot be null");
    }

    /**
     * Applies all of the configured constraints on the path. If one of them fails, then a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException}
     * is thrown.
     *
     * @param path the path to apply constraints to
     * @return the input path
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when a constraint fails
     */
    public String apply(String path) {
        pathConstraints.forEach(constraint -> constraint.apply(path));

        if (!fileNameConstraints.isEmpty() || !charConstraints.isEmpty()) {
            var segments = path.split("/");

            for (var segment : segments) {
                fileNameConstraints.forEach(constraint -> constraint.apply(segment, path));
                if (!charConstraints.isEmpty()) {
                    for (var c : segment.toCharArray()) {
                        charConstraints.forEach(constraint -> constraint.apply(c, path));
                    }
                }
            }
        }

        return path;
    }

    /**
     * Adds a new path constraint to the beginning of the list of path constraints
     *
     * @param pathConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor prependPathConstraint(PathConstraint pathConstraint) {
        this.pathConstraints.add(0, Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
        return this;
    }

    /**
     * Adds a new filename constraint to the beginning of the list of filename constraints
     *
     * @param fileNameConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor prependFileNameConstraint(FileNameConstraint fileNameConstraint) {
        this.fileNameConstraints.add(0, Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
        return this;
    }

    /**
     * Adds a new character constraint to the beginning of the list of character constraints
     *
     * @param charConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor prependCharConstraint(PathCharConstraint charConstraint) {
        this.charConstraints.add(0, Enforce.notNull(charConstraint, "charConstraint cannot be null"));
        return this;
    }

    /**
     * Adds a new path constraint to the end of the list of path constraints
     *
     * @param pathConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor appendPathConstraint(PathConstraint pathConstraint) {
        this.pathConstraints.add(Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
        return this;
    }

    /**
     * Adds a new filename constraint to the end of the list of filename constraints
     *
     * @param fileNameConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor appendFileNameConstraint(FileNameConstraint fileNameConstraint) {
        this.fileNameConstraints.add(Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
        return this;
    }

    /**
     * Adds a new character constraint to the end of the list of character constraints
     *
     * @param charConstraint constraint to add
     * @return this
     */
    public PathConstraintProcessor appendCharConstraint(PathCharConstraint charConstraint) {
        this.charConstraints.add(Enforce.notNull(charConstraint, "charConstraint cannot be null"));
        return this;
    }

}
