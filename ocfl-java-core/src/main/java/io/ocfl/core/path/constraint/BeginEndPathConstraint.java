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

package io.ocfl.core.path.constraint;

import io.ocfl.api.exception.PathConstraintException;
import io.ocfl.api.util.Enforce;

/**
 * Validates that a path or filename does or does not begin or and with a specified value
 */
public class BeginEndPathConstraint implements PathConstraint, FileNameConstraint {

    private enum Type {
        BEGIN {
            @Override
            boolean matches(String value, String path) {
                return path.startsWith(value);
            }
        },
        END {
            @Override
            boolean matches(String value, String path) {
                return path.endsWith(value);
            }
        };

        abstract boolean matches(String value, String path);
    }

    private final boolean mustMatch;
    private final Type type;
    private final String value;

    /**
     * Creates a new constraint that validates that a path DOES NOT end with a specified suffix
     *
     * @param suffix the suffix to test for
     * @return constraint
     */
    public static BeginEndPathConstraint mustNotEndWith(String suffix) {
        return new BeginEndPathConstraint(false, Type.END, suffix);
    }

    /**
     * Creates a new constraint that validates that a path DOES end with a specified suffix
     *
     * @param suffix the suffix to test for
     * @return constraint
     */
    public static BeginEndPathConstraint mustEndWith(String suffix) {
        return new BeginEndPathConstraint(true, Type.END, suffix);
    }

    /**
     * Creates a new constraint that validates that a path DOES NOT begin with a specified prefix
     *
     * @param prefix the prefix to test for
     * @return constraint
     */
    public static BeginEndPathConstraint mustNotBeginWith(String prefix) {
        return new BeginEndPathConstraint(false, Type.BEGIN, prefix);
    }

    /**
     * Creates a new constraint that validates that a path DOES begin with a specified prefix
     *
     * @param prefix the prefix to test for
     * @return constraint
     */
    public static BeginEndPathConstraint mustBeginWith(String prefix) {
        return new BeginEndPathConstraint(true, Type.BEGIN, prefix);
    }

    private BeginEndPathConstraint(boolean mustMatch, Type type, String value) {
        this.mustMatch = mustMatch;
        this.type = Enforce.notNull(type, "type cannot be null");
        this.value = Enforce.notBlank(value, "value cannot be blank");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String path) {
        if (type.matches(value, path)) {
            if (!mustMatch) {
                throw new PathConstraintException(
                        String.format("The path must not %s with %s. Path: %s", type, value, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(
                    String.format("The path must %s with %s but does not. Path: %s", type, value, path));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (type.matches(value, fileName)) {
            if (!mustMatch) {
                throw new PathConstraintException(
                        String.format("The filename '%s' must not %s with %s. Path: %s", fileName, type, value, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(String.format(
                    "The filename '%s' must %s with %s but does not. Path: %s", fileName, type, value, path));
        }
    }
}
