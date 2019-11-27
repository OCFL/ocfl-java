package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.ArrayList;
import java.util.List;

/**
 * A PathConstraintProcessor is used to apply a preconfigured set of constraints to a given path. If the path fails to
 * meet a constraint, a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException} is thrown.
 */
public class PathConstraintProcessor {

    private List<PathConstraint> pathConstraints;
    private List<FileNameConstraint> fileNameConstraints;
    private List<PathCharConstraint> charConstraints;

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

        public Builder pathConstraint(PathConstraint pathConstraint) {
            pathConstraints.add(Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
            return this;
        }

        public Builder pathConstraints(List<PathConstraint> pathConstraints) {
            this.pathConstraints = Enforce.notNull(pathConstraints, "pathConstraints cannot be null");
            return this;
        }

        public Builder fileNameConstraint(FileNameConstraint fileNameConstraint) {
            fileNameConstraints.add(Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
            return this;
        }

        public Builder fileNameConstraints(List<FileNameConstraint> fileNameConstraints) {
            this.fileNameConstraints = Enforce.notNull(fileNameConstraints, "fileNameConstraints cannot be null");
            return this;
        }

        public Builder charConstraint(PathCharConstraint charConstraint) {
            charConstraints.add(Enforce.notNull(charConstraint, "charConstraint cannot be null"));
            return this;
        }

        public Builder charConstraints(List<PathCharConstraint> charConstraints) {
            this.charConstraints = Enforce.notNull(charConstraints, "charConstraints cannot be null");
            return this;
        }

        public PathConstraintProcessor build() {
            return new PathConstraintProcessor(pathConstraints, fileNameConstraints, charConstraints);
        }

    }

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

    public PathConstraintProcessor prependPathConstraint(PathConstraint pathConstraint) {
        this.pathConstraints.add(0, Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
        return this;
    }

    public PathConstraintProcessor prependFileNameConstraint(FileNameConstraint fileNameConstraint) {
        this.fileNameConstraints.add(0, Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
        return this;
    }

    public PathConstraintProcessor prependCharConstraint(PathCharConstraint charConstraint) {
        this.charConstraints.add(0, Enforce.notNull(charConstraint, "charConstraint cannot be null"));
        return this;
    }

    public PathConstraintProcessor appendPathConstraint(PathConstraint pathConstraint) {
        this.pathConstraints.add(Enforce.notNull(pathConstraint, "pathConstraint cannot be null"));
        return this;
    }

    public PathConstraintProcessor appendFileNameConstraint(FileNameConstraint fileNameConstraint) {
        this.fileNameConstraints.add(Enforce.notNull(fileNameConstraint, "fileNameConstraint cannot be null"));
        return this;
    }

    public PathConstraintProcessor appendCharConstraint(PathCharConstraint charConstraint) {
        this.charConstraints.add(Enforce.notNull(charConstraint, "charConstraint cannot be null"));
        return this;
    }

}
