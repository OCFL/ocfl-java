package edu.wisc.library.ocfl.core.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class PathBiMapTest {

    private PathBiMap map;

    @BeforeEach
    public void setup() {
        map = new PathBiMap();
    }

    @Test
    public void basicMapOperations() {
        map.put("abc", "path");
        assertExists("abc", "path");
        assertNotExists("cba", "htap");
    }

    @Test
    public void keyShouldBeCaseInsensitive() {
        map.put("abc", "path");
        assertExists("AbC", "path");
    }

    @Test
    public void pathShouldNotBeCaseInsensitive() {
        map.put("abc", "path");
        assertNull(map.getFileId("Path"), "contains Path");
    }

    @Test
    public void fileIdWithMultiplePaths() {
        map.put("abc", "Path1");
        map.put("abc", "Path2");
        map.put("abc", "Path3");

        assertExists("abc", "Path1", "Path2", "Path3");
    }

    @Test
    public void removeByFileId() {
        map.put("abc", "path");
        map.put("abc", "path2");

        map.removeFileId("abc");

        assertNotExists("abc", "path", "path2");
    }

    @Test
    public void removeByPathWhenHasMultiplePathAndOnlyOneRemoved() {
        map.put("abc", "path");
        map.put("abc", "path2");

        map.removePath("path");

        assertExists("abc", "path2");
    }

    @Test
    public void removeByPathWhenHasMultiplePathAndBothRemoved() {
        map.put("abc", "path");
        map.put("abc", "path2");

        map.removePath("path");
        map.removePath("path2");

        assertNotExists("abc", "path", "path2");
    }

    private void assertExists(String fileId, String... paths) {
        for (var path : paths) {
            assertTrue(map.containsPath(path), "contains " + path);
            assertThat(map.getFileId(path), equalToIgnoringCase(fileId));
        }
        assertTrue(map.containsFileId(fileId), "contains " + fileId);
        assertThat(map.getPaths(fileId), contains(paths));
    }

    private void assertNotExists(String fileId, String... paths) {
        for (var path : paths) {
            assertFalse(map.containsPath(path), "contains " + path);
            assertNull(map.getFileId(path), "contains " + path);
        }
        assertFalse(map.containsFileId(fileId), "contains " + fileId);
        assertEquals(Collections.emptySet(), map.getPaths(fileId));
    }
}
