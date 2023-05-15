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

package io.ocfl.core.storage.filesystem;

import static io.ocfl.api.OcflConstants.OBJECT_NAMASTE_PREFIX;

import io.ocfl.api.exception.OcflIOException;
import io.ocfl.core.storage.common.OcflObjectRootDirIterator;
import io.ocfl.core.util.FileUtil;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.Iterator;

/**
 * Implementation of {@link OcflObjectRootDirIterator} that iterates over the filesystem
 */
public class FileSystemOcflObjectRootDirIterator extends OcflObjectRootDirIterator {

    private final Path root;

    public FileSystemOcflObjectRootDirIterator(Path root) {
        this.root = root;
    }

    @Override
    protected boolean isObjectRoot(String path) {
        try (var objectMarkers = Files.newDirectoryStream(
                root.resolve(path), p -> p.getFileName().toString().startsWith(OBJECT_NAMASTE_PREFIX))) {
            return objectMarkers.iterator().hasNext();
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    @Override
    protected Directory createDirectory(String path) {
        return new FileSystemDirectory(root, path);
    }

    private static class FileSystemDirectory implements Directory {

        private final Path root;
        private final DirectoryStream<Path> stream;
        private final Iterator<Path> children;

        FileSystemDirectory(Path root, String path) {
            try {
                this.root = root;
                this.stream = Files.newDirectoryStream(root.resolve(path));
                this.children = stream.iterator();
            } catch (IOException e) {
                throw new OcflIOException(e);
            }
        }

        @Override
        public String nextChildDirectory() {
            while (children.hasNext()) {
                var child = children.next();

                if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                    return FileUtil.pathToStringStandardSeparator(root.relativize(child));
                }
            }
            return null;
        }

        @Override
        public void close() {
            try {
                stream.close();
            } catch (IOException e) {
                // ignore
            }
        }
    }
}
