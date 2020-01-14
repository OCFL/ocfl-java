package edu.wisc.library.ocfl.test;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.User;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Scanner;

public final class TestHelper {

    private TestHelper() {

    }

    public static String fileToString(Path file) {
        try (var input = Files.newInputStream(file)) {
            return inputToString(input);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static String inputToString(InputStream inputStream) {
        var string = new Scanner(inputStream).useDelimiter("\\A").next();
        try {
            inputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return string;
    }

    public static CommitInfo commitInfo(String name, String address, String message) {
        return new CommitInfo().setMessage(message).setUser(new User().setName(name).setAddress(address));
    }

    public static InputStream inputStream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    public static InputStream inputStream(Path path) {
        try {
            return Files.newInputStream(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public static Path copyDir(Path source, Path target) {
        try (var files = Files.walk(source)) {
            files.forEach(f -> {
                try {
                    Files.copy(f, target.resolve(source.relativize(f)), StandardCopyOption.REPLACE_EXISTING);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return target;
    }

}
