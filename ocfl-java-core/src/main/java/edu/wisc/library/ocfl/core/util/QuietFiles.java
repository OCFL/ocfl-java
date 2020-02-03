package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.exception.RuntimeIOException;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * This class is just a wrapper around {@link Files} that converts IOExceptions into RuntimeIOExceptions.
 */
public final class QuietFiles {

    private QuietFiles() {

    }

    public static Path createDirectories(Path path) {
        try {
            return Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copy(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.copy(src, dst, copyOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copy(InputStream input, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.copy(input, dst);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static long size(Path path) {
        try {
            return Files.size(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
