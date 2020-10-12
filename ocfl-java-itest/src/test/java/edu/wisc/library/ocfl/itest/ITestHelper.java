package edu.wisc.library.ocfl.itest;

import com.fasterxml.jackson.core.PrettyPrinter;
import com.fasterxml.jackson.core.util.DefaultIndenter;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.DefaultOcflRepository;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleIdExtension;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.ObjectMappers;
import edu.wisc.library.ocfl.test.TestHelper;

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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public final class ITestHelper {

    private ITestHelper() {

    }

    public static void verifyDirectoryContentsSame(Path expected, Path actual) {
        verifyDirectoryContentsSame(expected, expected.getFileName().toString(), actual);
    }

    public static void verifyDirectoryContentsSame(Path expected, String expectDirName, Path actual) {
        assertTrue(Files.exists(actual), actual + " should exist");
        assertTrue(Files.isDirectory(actual), actual + "should be a directory");

        assertEquals(expectDirName, actual.getFileName().toString());

        var expectedPaths = listAllPaths(expected);
        var actualPaths = listAllPaths(actual);

        assertEquals(expectedPaths.size(), actualPaths.size(),
                comparingMessage(expected, actual));

        for (int i = 0; i < expectedPaths.size(); i++) {
            var expectedPath = expectedPaths.get(i);
            var actualPath = actualPaths.get(i);

            assertEquals(expected.relativize(expectedPath).toString(), actual.relativize(actualPath).toString());

            if (Files.isDirectory(expectedPath)) {
                assertTrue(Files.isDirectory(actualPath), actualPath + " should be a directory");
            } else {
                assertTrue(Files.isRegularFile(actualPath), actualPath + " should be a file");
                assertEquals(computeDigest(expectedPath), computeDigest(actualPath),
                        comparingMessage(expectedPath, actualPath, actualPath));
            }
        }
    }

    public static List<Path> listAllPaths(Path root) {
        var allPaths = new TreeSet<Path>();

        try (var walk = Files.walk(root)) {
            walk.filter(p -> {
                var pStr = p.toString();
                return !(pStr.contains(".gitkeep")
                        || pStr.contains("deposit")
                        // TODO remove this once layout an extensions are finalized
                        || pStr.contains("ocfl_layout")
                        || pStr.contains(HashedTruncatedNTupleIdExtension.EXTENSION_NAME)
                        || pStr.contains(HashedTruncatedNTupleExtension.EXTENSION_NAME));
            }).filter(Files::isRegularFile).forEach(allPaths::add);
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
