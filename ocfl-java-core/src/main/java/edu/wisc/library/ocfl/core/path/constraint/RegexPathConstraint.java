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

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.util.Enforce;
import java.util.regex.Pattern;

/**
 * Validates that a path or filename does or does not match a regex
 */
public class RegexPathConstraint implements PathConstraint, FileNameConstraint {

    private final Pattern pattern;
    private final boolean mustMatch;

    /**
     * Creates a new constraint that validates paths do not match the regex
     *
     * @param pattern the regex that paths must not match
     * @return constraint
     */
    public static RegexPathConstraint mustNotContain(Pattern pattern) {
        return new RegexPathConstraint(false, pattern);
    }

    /**
     * Creates a new constraint that validates paths do match the regex
     *
     * @param pattern the regex that paths must match
     * @return constraint
     */
    public static RegexPathConstraint mustContain(Pattern pattern) {
        return new RegexPathConstraint(true, pattern);
    }

    public RegexPathConstraint(boolean mustMatch, Pattern pattern) {
        this.mustMatch = mustMatch;
        this.pattern = Enforce.notNull(pattern, "pattern cannot be null");
    }

    @Override
    public void apply(String path) {
        if (pattern.matcher(path).matches()) {
            if (!mustMatch) {
                throw new PathConstraintException(
                        String.format("The path contains an invalid sequence %s. Path: %s", pattern, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(
                    String.format("The path must contain a sequence %s but does not. Path: %s", pattern, path));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (pattern.matcher(fileName).matches()) {
            if (!mustMatch) {
                throw new PathConstraintException(String.format(
                        "The filename '%s' contains an invalid sequence %s. Path: %s", fileName, pattern, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(String.format(
                    "The filename '%s' must contain a sequence %s but does not. Path: %s", fileName, pattern, path));
        }
    }
}
