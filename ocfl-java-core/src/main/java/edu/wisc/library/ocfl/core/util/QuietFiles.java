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

package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.exception.RuntimeIOException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * This class is just a wrapper around {@link Files} that converts IOExceptions into RuntimeIOExceptions.
 */
public final class QuietFiles {

    private QuietFiles() {

    }

    public static Path createDirectories(Path path) {
        try {
            return Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copy(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.copy(src, dst, copyOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copy(InputStream input, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.copy(input, dst);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
