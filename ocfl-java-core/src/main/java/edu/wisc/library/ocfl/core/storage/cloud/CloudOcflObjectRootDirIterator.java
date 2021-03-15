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

package edu.wisc.library.ocfl.core.storage.cloud;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.storage.OcflObjectRootDirIterator;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.util.Iterator;

/**
 * Implementation of {@link OcflObjectRootDirIterator} that iterates over cloud objects
 */
public class CloudOcflObjectRootDirIterator extends OcflObjectRootDirIterator {

    private static final String EXT_SUFFIX = "/" + OcflConstants.EXTENSIONS_DIR;
    private static final String EXT_PREFIX = OcflConstants.EXTENSIONS_DIR + "/";

    private final CloudClient cloudClient;

    public CloudOcflObjectRootDirIterator(String start, CloudClient cloudClient) {
        super(start);
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
    }

    @Override
    protected boolean isObjectRoot(String path) {
        var listResult = cloudClient.list(FileUtil.pathJoinFailEmpty(path, OCFL_OBJECT_MARKER_PREFIX));
        return !listResult.getObjects().isEmpty();
    }

    @Override
    protected boolean shouldSkip(String path) {
        return path.endsWith(EXT_SUFFIX) || EXT_PREFIX.equals(path);
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
                return childDirectories.next().getPath();
            }
            return null;
        }

        @Override
        public void close() {
            // noop
        }

    }

}
