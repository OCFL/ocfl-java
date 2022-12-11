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
import java.nio.charset.StandardCharsets;

/**
 * Validates that a path or filename are not longer than a fixed number of characters or bytes
 */
public class PathLengthConstraint implements PathConstraint, FileNameConstraint {

    public enum Type {
        CHARACTERS("characters"),
        BYTES("bytes");

        private final String value;

        Type(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return value;
        }
    }

    private final int maxLength;
    private final Type type;

    /**
     * Creates a new constraint that limits the number of characters in a path
     *
     * @param maxLength maximum number of characters
     * @return constraint
     */
    public static PathLengthConstraint maxChars(int maxLength) {
        return new PathLengthConstraint(maxLength, Type.CHARACTERS);
    }

    /**
     * Creates a new constraint that limits the number of bytes in a path
     *
     * @param maxLength maximum number of bytes
     * @return constraint
     */
    public static PathLengthConstraint maxBytes(int maxLength) {
        return new PathLengthConstraint(maxLength, Type.BYTES);
    }

    public PathLengthConstraint(int maxLength, Type type) {
        this.maxLength = Enforce.expressionTrue(maxLength > 0, maxLength, "maxLength must be greater than 0");
        this.type = Enforce.notNull(type, "type cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String path) {
        if (type == Type.CHARACTERS) {
            validateLength(path.length(), message(path));
        } else {
            validateLength(path.getBytes(StandardCharsets.UTF_8).length, message(path));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (type == Type.CHARACTERS) {
            validateLength(fileName.length(), message(fileName, path));
        } else {
            validateLength(fileName.getBytes(StandardCharsets.UTF_8).length, message(fileName, path));
        }
    }

    private void validateLength(int length, String message) {
        if (length > maxLength) {
            throw new PathConstraintException(message);
        }
    }

    private String message(String path) {
        return String.format("The path is longer than %s %s. Path: %s", maxLength, type, path);
    }

    private String message(String fileName, String path) {
        return String.format("The filename '%s' is longer than %s %s. Path: %s", fileName, maxLength, type, path);
    }
}
