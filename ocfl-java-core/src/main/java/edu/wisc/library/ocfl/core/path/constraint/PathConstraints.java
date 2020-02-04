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
