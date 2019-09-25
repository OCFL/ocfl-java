package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class NamasteTypeFile {

    private static final String NAMASTE_PREFIX = "0=";

    private final String tagValue;

    public NamasteTypeFile(String tagValue) {
        this.tagValue = Enforce.notBlank(tagValue, "tagValue cannot be blank");
    }

    public String fileName() {
        return NAMASTE_PREFIX + tagValue;
    }

    public String fileContent() {
        return tagValue + "\n";
    }

    public void writeFile(Path directory) {
        try {
            Files.writeString(directory.resolve(fileName()), fileContent());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
