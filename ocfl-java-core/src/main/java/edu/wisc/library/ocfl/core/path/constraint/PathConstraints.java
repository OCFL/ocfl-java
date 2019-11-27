package edu.wisc.library.ocfl.core.path.constraint;

import java.util.regex.Pattern;

public final class PathConstraints {

    private PathConstraints() {

    }

    private static final PathConstraintProcessor NON_EMPTY_DOT_CONSTRAINTS = PathConstraintProcessor.builder()
            .fileNameConstraint(new NonEmptyFileNameConstraint())
            .fileNameConstraint(RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$")))
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

}
