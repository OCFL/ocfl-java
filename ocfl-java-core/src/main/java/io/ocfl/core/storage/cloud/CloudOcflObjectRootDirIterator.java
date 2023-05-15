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

package io.ocfl.core.storage.cloud;

import static io.ocfl.api.OcflConstants.OBJECT_NAMASTE_PREFIX;

import io.ocfl.api.util.Enforce;
import io.ocfl.core.storage.common.OcflObjectRootDirIterator;
import io.ocfl.core.util.FileUtil;

import java.util.Iterator;

/**
 * Implementation of {@link OcflObjectRootDirIterator} that iterates over cloud objects
 */
public class CloudOcflObjectRootDirIterator extends OcflObjectRootDirIterator {

    private final CloudClient cloudClient;

    public CloudOcflObjectRootDirIterator(CloudClient cloudClient) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
    }

    @Override
    protected boolean isObjectRoot(String path) {
        var listResult = cloudClient.list(FileUtil.pathJoinFailEmpty(path, OBJECT_NAMASTE_PREFIX));
        return !listResult.getObjects().isEmpty();
    }

    @Override
    protected Directory createDirectory(String path) {
        return new CloudDirectory(path);
    }

    private class CloudDirectory implements Directory {

        private final Iterator<ListResult.DirectoryListing> childDirectories;

        CloudDirectory(String path) {
            var listResult = cloudClient.listDirectory(path);
            childDirectories = listResult.getDirectories().iterator();
        }

        @Override
        public String nextChildDirectory() {
            if (childDirectories.hasNext()) {
                // the path will have a trailing `/`
                var path = childDirectories.next().getPath();
                return path.substring(0, path.length() - 1);
            }
            return null;
        }

        @Override
        public void close() {
            // noop
        }
    }
}
