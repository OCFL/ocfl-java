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
        return "ListResult{" +
                "objects=" + objects +
                ", directories=" + directories +
                '}';
    }

    /**
     * Encapsulates an object key and its filename. An object's filename is the portion of its key that's after the final
     * '/' separator.
     */
    public static class ObjectListing {

        private String key;
        private String fileName;

        /**
         * The key the object is stored at.
         *
         * @return object key
         */
        public String getKey() {
            return key;
        }

        public ObjectListing setKey(String key) {
            this.key = key;
            return this;
        }

        /**
         * The object's filename, the final portion of its key that's after the final '/' separator
         *
         * @return object filename
         */
        public String getFileName() {
            return fileName;
        }

        public ObjectListing setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        @Override
        public String toString() {
            return "ObjectListing{" +
                    "key='" + key + '\'' +
                    ", fileName='" + fileName + '\'' +
                    '}';
        }
    }

    /**
     * Encapsulates a virtual directory. Directories do not exist in object stores. This object represents an object key
     * prefix.
     */
    public static class DirectoryListing {

        private String path;
        private String fileName;

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
         * The directory's filename, the final portion of the prefix that's after the final '/' separator
         *
         * @return object filename
         */
        public String getFileName() {
            return fileName;
        }

        public DirectoryListing setFileName(String fileName) {
            this.fileName = fileName;
            return this;
        }

        @Override
        public String toString() {
            return "DirectoryListing{" +
                    "path='" + path + '\'' +
                    ", fileName='" + fileName + '\'' +
                    '}';
        }
    }

}
