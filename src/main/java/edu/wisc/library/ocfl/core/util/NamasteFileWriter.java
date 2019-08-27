package edu.wisc.library.ocfl.core.util;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * Simple class to write out a namaste file to a given directory
 */
public class NamasteFileWriter {

    private static final String NAMASTE_PREFIX = "0=";

    public void writeFile(Path directory, String version) {
        try {
            Files.writeString(directory.resolve(namasteFileName(version)), version + "\n");
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public String namasteFileName(String version) {
        return NAMASTE_PREFIX + version;
    }

}
