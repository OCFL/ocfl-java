package io.ocfl.itest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.ocfl.api.OcflRepository;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.core.DefaultOcflRepository;
import io.ocfl.core.inventory.InventoryMapper;
import io.ocfl.core.util.DigestUtil;
import io.ocfl.core.util.ObjectMappers;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Clock;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;
import java.util.function.Supplier;

public final class ITestHelper {

    private static final String OCFL_SPEC_FILE = "ocfl_1.1.md";

    private ITestHelper() {}

    public static void verifyDirectoryContentsSame(Path expected, Path actual) {
        verifyDirectoryContentsSame(expected, expected.getFileName().toString(), actual);
    }

    public static void verifyDirectoryContentsSame(Path expected, String expectDirName, Path actual) {
        assertTrue(Files.exists(actual), actual + " should exist");
        assertTrue(Files.isDirectory(actual), actual + "should be a directory");

        assertEquals(expectDirName, actual.getFileName().toString());

        var expectedPaths = listAllPaths(expected);
        var actualPaths = listAllPaths(actual);

        assertEquals(expectedPaths.size(), actualPaths.size(), comparingMessage(expected, actual));

        for (int i = 0; i < expectedPaths.size(); i++) {
            var expectedPath = expectedPaths.get(i);
            var actualPath = actualPaths.get(i);

            assertEquals(
                    expected.relativize(expectedPath).toString(),
                    actual.relativize(actualPath).toString());

            if (Files.isDirectory(expectedPath)) {
                assertTrue(Files.isDirectory(actualPath), actualPath + " should be a directory");
            } else {
                assertTrue(Files.isRegularFile(actualPath), actualPath + " should be a file");
                assertEquals(
                        computeDigest(expectedPath),
                        computeDigest(actualPath),
                        comparingMessage(expectedPath, actualPath, actualPath));
            }
        }
    }

    public static List<Path> listAllPaths(Path root) {
        var allPaths = new TreeSet<Path>();

        try (var walk = Files.walk(root)) {
            walk.filter(p -> {
                        var filename = p.getFileName().toString();
                        return !filename.equals(".gitkeep")
                                && !filename.equals(OCFL_SPEC_FILE)
                                && !filename.equals("ocfl_1.0.txt");
                    })
                    .filter(Files::isRegularFile)
                    .forEach(allPaths::add);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        return new ArrayList<>(allPaths);
    }

    public static String computeDigest(Path path) {
        return DigestUtil.computeDigestHex(DigestAlgorithm.md5, path);
    }

    public static String comparingMessage(Object o1, Object o2) {
        return String.format("Comparing %s and %s", o1, o2);
    }

    public static Supplier<String> comparingMessage(Object o1, Object o2, Path actualPath) {
        return () -> String.format("Comparing %s and %s:\n\n%s", o1, o2, TestHelper.fileToString(actualPath));
    }

    public static void fixTime(OcflRepository repository, String timestamp) {
        ((DefaultOcflRepository) repository).setClock(Clock.fixed(Instant.parse(timestamp), ZoneOffset.UTC));
    }

    public static Path expectedOutputPath(String repoName, String name) {
        return Paths.get("src/test/resources/expected/output", repoName, name);
    }

    public static Path expectedRepoPath(String name) {
        return Paths.get("src/test/resources/expected/repos", name);
    }

    public static Path sourceObjectPath(String objectId, String version) {
        return Paths.get("src/test/resources/sources/objects", objectId, version);
    }

    public static Path sourceRepoPath(String repo) {
        return Paths.get("src/test/resources/sources/repos", repo);
    }

    public static InventoryMapper testInventoryMapper() {
        return new InventoryMapper(prettyPrintMapper());
    }

    public static ObjectMapper prettyPrintMapper() {
        return ObjectMappers.prettyPrintMapper().setDefaultPrettyPrinter(prettyPrinter());
    }

    public static PrettyPrinter prettyPrinter() {
        return new DefaultPrettyPrinter().withObjectIndenter(new DefaultIndenter("  ", "\n"));
    }

    public static InputStream streamString(String value) {
        return new ByteArrayInputStream(value.getBytes(StandardCharsets.UTF_8));
    }
}
