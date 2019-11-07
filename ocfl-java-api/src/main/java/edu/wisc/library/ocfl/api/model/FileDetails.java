package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates a filePath with all of its fixity information.
 */
public class FileDetails {

    private String path;
    private String storageRelativePath;
    private Map<String, String> fixity;

    public FileDetails() {
        this.fixity = new HashMap<>();
    }

    /**
     * The file's logical path within the object
     *
     * @return logical path
     */
    public String getPath() {
        return path;
    }

    public FileDetails setPath(String path) {
        this.path = path;
        return this;
    }

    /**
     * The file's path relative to the storage root
     *
     * @return storage relative path
     */
    public String getStorageRelativePath() {
        return storageRelativePath;
    }

    public FileDetails setStorageRelativePath(String storageRelativePath) {
        this.storageRelativePath = storageRelativePath;
        return this;
    }

    /**
     * Map of digest algorithm to digest value.
     *
     * @return digest map
     */
    public Map<String, String> getFixity() {
        return fixity;
    }

    public FileDetails setFixity(Map<String, String> fixity) {
        this.fixity = fixity;
        return this;
    }

    public FileDetails addDigest(String algorithm, String value) {
        Enforce.notBlank(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be null");
        this.fixity.put(algorithm, value);
        return this;
    }

    @Override
    public String toString() {
        return "FileDetails{" +
                "path='" + path + '\'' +
                "storageRelativePath='" + storageRelativePath + '\'' +
                ", fixity=" + fixity +
                '}';
    }

}
