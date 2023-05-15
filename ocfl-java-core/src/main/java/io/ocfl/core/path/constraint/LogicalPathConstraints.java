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

package io.ocfl.core.path.constraint;

import java.util.regex.Pattern;

/**
 * Describes the constraints that are applied to logical paths.
 */
public final class LogicalPathConstraints {

    private LogicalPathConstraints() {}

    private static final PathConstraint LEADING_SLASH_CONSTRAINT = BeginEndPathConstraint.mustNotBeginWith("/");
    private static final PathConstraint TRAILING_SLASH_CONSTRAINT = BeginEndPathConstraint.mustNotEndWith("/");
    private static final FileNameConstraint NOT_EMPTY_CONSTRAINT = new NonEmptyFileNameConstraint();
    private static final FileNameConstraint NO_DOTS_CONSTRAINT =
            RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$"));

    private static final PathConstraintProcessor CONSTRAINTS_WITHOUT_BACKSLASH_CHECK = PathConstraintProcessor.builder()
            .pathConstraint(LEADING_SLASH_CONSTRAINT)
            .pathConstraint(TRAILING_SLASH_CONSTRAINT)
            .fileNameConstraint(NOT_EMPTY_CONSTRAINT)
            .fileNameConstraint(NO_DOTS_CONSTRAINT)
            .build();

    private static final PathConstraintProcessor CONSTRAINTS_WITH_BACKSLASH_CHECK = PathConstraintProcessor.builder()
            .pathConstraint(LEADING_SLASH_CONSTRAINT)
            .pathConstraint(TRAILING_SLASH_CONSTRAINT)
            .fileNameConstraint(NOT_EMPTY_CONSTRAINT)
            .fileNameConstraint(NO_DOTS_CONSTRAINT)
            .charConstraint(new BackslashPathSeparatorConstraint())
            .build();

    /**
     * Logical paths may not:
     *
     * <ul>
     *     <li>Begin or end with a '/'</li>
     *     <li>Contain empty filenames</li>
     *     <li>Contain the filenames '.' or '..'</li>
     * </ul>
     *
     * @return logical path constraints
     */
    public static PathConstraintProcessor constraints() {
        return CONSTRAINTS_WITHOUT_BACKSLASH_CHECK;
    }

    /**
     * Logical paths may not:
     *
     * <ul>
     *     <li>Begin or end with a '/'</li>
     *     <li>Contain empty filenames</li>
     *     <li>Contain the filenames '.' or '..'</li>
     *     <li>Use backslashes as path separators</li>
     * </ul>
     *
     * <p>This backslash constraint is not a spec constraint, but is in some cases necessary to enforce when running
     * on Windows.
     *
     * @return logical path constraints
     */
    public static PathConstraintProcessor constraintsWithBackslashCheck() {
        return CONSTRAINTS_WITH_BACKSLASH_CHECK;
    }
}
