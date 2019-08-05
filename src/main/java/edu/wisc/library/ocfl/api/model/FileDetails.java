package edu.wisc.library.ocfl.api.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.HashMap;
import java.util.Map;

public class FileDetails {

    private String filePath;
    private Map<String, String> fixity;

    public FileDetails() {
        this.fixity = new HashMap<>();
    }

    public String getFilePath() {
        return filePath;
    }

    public FileDetails setFilePath(String filePath) {
        this.filePath = filePath;
        return this;
    }

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
                "filePath='" + filePath + '\'' +
                ", fixity=" + fixity +
                '}';
    }

}
