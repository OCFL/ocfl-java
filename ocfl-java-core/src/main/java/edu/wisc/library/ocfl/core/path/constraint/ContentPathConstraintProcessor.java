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

public interface ContentPathConstraintProcessor {

    /**
     * Applies the configured path constrains to the content path and storage path. If any constraints fail,
     * a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException} is thrown.
     *
     * @param contentPath the content path relative a version's content directory
     * @param storagePath the content path relative the OCFL repository root
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when a constraint fails
     */
    void apply(String contentPath, String storagePath);

    /**
     * Applies the configured path constrains to the content path and storage path. If any constraints fail,
     * a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException} is thrown.
     *
     * @param contentPath the content path relative a version's content directory
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when a constraint fails
     */
    void apply(String contentPath);

}
