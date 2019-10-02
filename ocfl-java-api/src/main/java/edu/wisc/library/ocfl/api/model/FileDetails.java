package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.HashMap;
import java.util.Map;

/**
 * Encapsulates a filePath with all of its fixity information.
 */
public class FileDetails {

    private String objectRelativePath;
    private String storageRelativePath;
    private Map<String, String> fixity;

    public FileDetails() {
        this.fixity = new HashMap<>();
    }

    /**
     * The file's path relative to the object root
     */
    public String getObjectRelativePath() {
        return objectRelativePath;
    }

    public FileDetails setObjectRelativePath(String objectRelativePath) {
        this.objectRelativePath = objectRelativePath;
        return this;
    }

    /**
     * The file's path relative to the storage root
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
                "objectRelativePath='" + objectRelativePath + '\'' +
                "storageRelativePath='" + storageRelativePath + '\'' +
                ", fixity=" + fixity +
                '}';
    }

}
