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

package edu.wisc.library.ocfl.api;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

public enum OcflOption {

    /**
     * Instructs an update operation to overwrite existing files in an object.
     */
    OVERWRITE,
    /**
     * Instructs an update operation to move files into the repository instead of copying them.
     */
    MOVE_SOURCE,
    /**
     * Disables validations that ensure the integrity of OCFL objects
     */
    NO_VALIDATION;

    /**
     * Transforms a varargs of options into a set
     *
     * @param options options
     * @return set of options
     */
    public static Set<OcflOption> toSet(OcflOption... options) {
        return new HashSet<>(Arrays.asList(options));
    }

    /**
     * Returns true if the set of options contains the specified option
     *
     * @param test the option to look for
     * @param options the set of options
     * @return true if the specified option is in the set
     */
    public static boolean contains(OcflOption test, OcflOption... options) {
        if (options == null) {
            return false;
        }

        for (var option : options) {
            if (option == test) {
                return true;
            }
        }

        return false;
    }
}
