package edu.wisc.library.ocfl.core.path.constraint;

import java.util.regex.Pattern;

public final class PathConstraints {

    private PathConstraints() {

    }

    private static final PathConstraintProcessor NON_EMPTY_DOT_CONSTRAINTS = PathConstraintProcessor.builder()
            .fileNameConstraint(new NonEmptyFileNameConstraint())
            .fileNameConstraint(RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$")))
            .build();

    private static final PathConstraintProcessor LOGICAL_PATH_CONSTRAINTS = PathConstraintProcessor.builder()
            .fileNameConstraint(new NonEmptyFileNameConstraint())
            .fileNameConstraint(RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$")))
            .charConstraint(new BackslashPathSeparatorConstraint())
            .build();

    /**
     * Applies the following constraints:
     *
     * <ul>
     *     <li>Cannot have empty filenames</li>
     *     <li>Cannot have the filenames . or ..</li>
     * </ul>
     *
     * @return constraint processor
     */
    public static PathConstraintProcessor noEmptyOrDotFilenames() {
        return NON_EMPTY_DOT_CONSTRAINTS;
    }

    /**
     * Logical paths may not contain empty filenames, '.' or '..' segments, or use backslashes as separators.
     *
     * @return logical path constraints
     */
    public static PathConstraintProcessor logicalPathConstraints() {
        return LOGICAL_PATH_CONSTRAINTS;
    }

}
