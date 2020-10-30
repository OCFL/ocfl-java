package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.exception.OcflInputException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.InventoryType;
import edu.wisc.library.ocfl.api.model.VersionNum;
import org.hamcrest.Matchers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;
import java.util.Map;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalToIgnoringCase;
import static org.junit.jupiter.api.Assertions.*;

public class InventoryBuilderTest {

    private InventoryBuilder builder;
    private VersionBuilder versionBuilder;

    @BeforeEach
    public void setup() {
        this.builder = Inventory.builder()
                .id("id")
                .type(InventoryType.OCFL_1_0)
                .digestAlgorithm(DigestAlgorithm.sha512)
                .head(VersionNum.fromString("v1"))
                .objectRootPath("root");

        this.versionBuilder = Version.builder().created(OffsetDateTime.now());
    }

    @Test
    public void shouldAddFileToManifestWhenNotAlreadyThere() {
        builder.addFileToManifest("abc", "path");
        builder.addFileToManifest("abc", "path2");

        assertExists("abc", "path", "path2");
        assertExists(builder.build(), "abc", "path", "path2");
    }

    @Test
    public void shouldAddMultipleFiles() {
        builder.addFileToManifest("abc", "path");
        builder.addFileToManifest("aBc", "path2");
        builder.addFileToManifest("cba", "other/path");

        assertExists("abc", "path", "path2");
        assertExists("cba", "other/path");

        var inventory = builder.build();

        assertExists(inventory, "abc", "path", "path2");
        assertExists(inventory, "cba", "other/path");
    }

    @Test
    public void shouldRemoveAllPathsById() {
        builder.addFileToManifest("abc", "path");
        builder.addFileToManifest("aBc", "path2");
        builder.addFileToManifest("cba", "other/path");

        builder.removeFileId("abc");

        assertNotExists("abc", "path", "path2");
        assertExists("cba", "other/path");

        var inventory = builder.build();

        assertNotExists(inventory, "abc", "path", "path2");
        assertExists(inventory, "cba", "other/path");
    }

    @Test
    public void shouldRemoveByPath() {
        builder.addFileToManifest("abc", "path");
        builder.addFileToManifest("aBc", "path2");
        builder.addFileToManifest("cba", "other/path");

        builder.removeContentPath("path");
        builder.removeContentPath("other/path");

        assertExists("abc", "path2");
        assertNotExists("cba", "other/path");

        var inventory = builder.build();

        assertExists(inventory, "abc", "path2");
        assertNotExists(inventory, "cba", "other/path");
    }

    @Test
    public void shouldAddFixityForFileInManifest() {
        builder.addFileToManifest("abc", "path");

        builder.addFixityForFile("path", DigestAlgorithm.md5, "md5_123");
        builder.addFixityForFile("path", DigestAlgorithm.sha1, "sha1_123");

        var inventory = builder.build();

        assertFixity(inventory, "path", Map.of(
                DigestAlgorithm.md5, "md5_123",
                DigestAlgorithm.sha1, "sha1_123"
        ));
    }

    @Test
    public void shouldNotAddFixityWhenFileNotInManifest() {
        assertThat(assertThrows(OcflInputException.class, () -> {
            builder.addFixityForFile("path", DigestAlgorithm.md5, "md5_123");
        }).getMessage(), Matchers.containsString("Cannot add fixity information for"));
    }

    @Test
    public void shouldRemoveFixityWhenFileRemovedFromManifest() {
        builder.addFileToManifest("abc", "path");

        builder.addFixityForFile("path", DigestAlgorithm.md5, "md5_123");
        builder.addFixityForFile("path", DigestAlgorithm.sha1, "sha1_123");

        builder.removeFileId("abc");

        var inventory = builder.build();

        assertFixity(inventory, "path", Map.of());
    }

    @Test
    public void shouldAddHeadVersionWhenNotMutableAndV0() {
        var version = versionBuilder.build();
        var inventory = builder.head(VersionNum.fromString("v0"))
                .addHeadVersion(version)
                .build();

        assertEquals(VersionNum.fromString("v1"), inventory.getHead());
        assertSame(version, inventory.getHeadVersion());
    }

    @Test
    public void shouldAddHeadVersionWhenNotMutableAndV3() {
        var version = versionBuilder.build();
        var inventory = builder.head(VersionNum.fromString("v3"))
                .addHeadVersion(version)
                .build();

        assertEquals(VersionNum.fromString("v4"), inventory.getHead());
        assertSame(version, inventory.getHeadVersion());
    }

    @Test
    public void shouldAddHeadVersionWhenMutableAndV1NoRevisions() {
        var version = versionBuilder.build();
        var inventory = builder.head(VersionNum.fromString("v1"))
                .mutableHead(true)
                .addHeadVersion(version)
                .build();

        assertEquals(VersionNum.fromString("v2"), inventory.getHead());
        assertEquals(RevisionNum.fromString("r1"), inventory.getRevisionNum());
        assertSame(version, inventory.getHeadVersion());
    }

    @Test
    public void shouldAddHeadVersionWhenMutableAndV1R3() {
        var version = versionBuilder.build();
        var inventory = builder.head(VersionNum.fromString("v3"))
                .mutableHead(true)
                .revisionNum(RevisionNum.fromString("r3"))
                .addHeadVersion(version)
                .build();

        assertEquals(VersionNum.fromString("v3"), inventory.getHead());
        assertEquals(RevisionNum.fromString("r4"), inventory.getRevisionNum());
        assertSame(version, inventory.getHeadVersion());
    }

    @Test
    public void shouldClearFixity() {
        builder.addFileToManifest("1", "path")
                .addFileToManifest("2", "path2")
                .addFixityForFile("path", DigestAlgorithm.md5, "md5_1")
                .addFixityForFile("path2", DigestAlgorithm.md5, "md5_2");

        assertEquals("md5_1", builder.getFileFixity("1", DigestAlgorithm.md5));
        assertEquals("md5_2", builder.getFileFixity("2", DigestAlgorithm.md5));

        builder.clearFixity();

        assertNull(builder.getFileFixity("1", DigestAlgorithm.md5));
        assertNull(builder.getFileFixity("2", DigestAlgorithm.md5));
    }

    private void assertFixity(Inventory inventory, String contentPath, Map<DigestAlgorithm, String> expected) {
        var fixity = inventory.getFixityForContentPath(contentPath);
        assertEquals(expected, fixity);
    }

    private void assertExists(String fileId, String... paths) {
        for (var path : paths) {
            assertTrue(builder.containsContentPath(path), "contains " + path);
            assertThat(builder.getFileId(path), equalToIgnoringCase(fileId));
        }
        assertTrue(builder.containsFileId(fileId), "contains " + fileId);
        assertThat(builder.getContentPaths(fileId), contains(paths));
    }

    private void assertNotExists(String fileId, String... paths) {
        for (var path : paths) {
            assertFalse(builder.containsContentPath(path), "contains " + path);
            assertNull(builder.getFileId(path), "contains " + path);
        }
        assertFalse(builder.containsFileId(fileId), "contains " + fileId);
        assertEquals(Collections.emptySet(), builder.getContentPaths(fileId));
    }

    private void assertExists(Inventory inventory, String fileId, String... paths) {
        for (var path : paths) {
            assertThat(inventory.getFileId(path), equalToIgnoringCase(fileId));
        }
        assertTrue(inventory.manifestContainsFileId(fileId), "contains " + fileId);
        assertThat(inventory.getContentPaths(fileId), contains(paths));
    }

    private void assertNotExists(Inventory inventory, String fileId, String... paths) {
        for (var path : paths) {
            assertNull(inventory.getFileId(path), "contains " + path);
        }
        assertFalse(inventory.manifestContainsFileId(fileId), "contains " + fileId);
        assertEquals(Collections.emptySet(), inventory.getContentPaths(fileId));
    }

}
