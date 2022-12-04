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

import edu.wisc.library.ocfl.api.util.Enforce;
import java.util.Collections;
import java.util.List;

/**
 * Encapsulates the results of a list operation
 */
public class ListResult {

    private List<ObjectListing> objects;
    private List<DirectoryListing> directories;

    public ListResult() {
        this.objects = Collections.emptyList();
        this.directories = Collections.emptyList();
    }

    /**
     * The list of objects that were returned by the list operation
     *
     * @return objects
     */
    public List<ObjectListing> getObjects() {
        return objects;
    }

    public ListResult setObjects(List<ObjectListing> objects) {
        this.objects = Enforce.notNull(objects, "objects cannot be null");
        return this;
    }

    /**
     * The directories that were returned by the list operation
     *
     * @return directories
     */
    public List<DirectoryListing> getDirectories() {
        return directories;
    }

    public ListResult setDirectories(List<DirectoryListing> directories) {
        this.directories = Enforce.notNull(directories, "directories cannot be null");
        return this;
    }

    @Override
    public String toString() {
        return "ListResult{" + "objects=" + objects + ", directories=" + directories + '}';
    }

    /**
     * Encapsulates an object key and its suffix. An object's suffix is the portion of its key that's after the key prefix
     * the list operation was on
     */
    public static class ObjectListing {

        private CloudObjectKey key;
        private String keySuffix;

        /**
         * The key the object is stored at.
         *
         * @return object key
         */
        public CloudObjectKey getKey() {
            return key;
        }

        public ObjectListing setKey(CloudObjectKey key) {
            this.key = key;
            return this;
        }

        /**
         * The object's key suffix, the portion of the key that appears after the key prefix the list operation was on
         *
         * @return object key suffix
         */
        public String getKeySuffix() {
            return keySuffix;
        }

        public ObjectListing setKeySuffix(String keySuffix) {
            this.keySuffix = keySuffix;
            return this;
        }

        @Override
        public String toString() {
            return "ObjectListing{" + "key='" + key + '\'' + ", keySuffix='" + keySuffix + '\'' + '}';
        }
    }

    /**
     * Encapsulates a virtual directory. Directories do not exist in object stores. This object represents an object key
     * prefix.
     */
    public static class DirectoryListing {

        private String path;

        /**
         * The object key prefix
         *
         * @return object key prefix
         */
        public String getPath() {
            return path;
        }

        public DirectoryListing setPath(String path) {
            this.path = path;
            return this;
        }

        /**
         * Return the virtual name of the directory. For example, if the path is `a/b/`, then the name is `b`.
         *
         * @return directory name
         */
        public String getName() {
            var name = path;
            if (name.endsWith("/")) {
                name = name.substring(0, name.length() - 1);
            }
            return name.substring(Math.max(0, name.lastIndexOf("/") + 1));
        }

        @Override
        public String toString() {
            return "DirectoryListing{" + "path='" + path + '\'' + '}';
        }
    }
}
