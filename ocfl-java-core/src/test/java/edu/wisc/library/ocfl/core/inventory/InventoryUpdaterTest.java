package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.OcflConfig;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.Collections;

import static org.junit.jupiter.api.Assertions.*;

public class InventoryUpdaterTest {

    private Inventory inventory;
    private InventoryUpdater.Builder builder;

    @BeforeEach
    public void setup() {
        inventory = Inventory.builder(Inventory.stubInventory("id", new OcflConfig(), "root"))
                .addFileToManifest("file1", "v1/content/file1p")
                .addFileToManifest("file2", "v1/content/file2p")
                .addHeadVersion(Version.builder()
                        .addFile("file1", "file1p")
                        .addFile("file2", "file2p")
                        .created(OffsetDateTime.now())
                        .build())
                .addHeadVersion(Version.builder()
                        .addFile("file1", "file1p")
                        .created(OffsetDateTime.now())
                        .build())
                .build();
        builder = InventoryUpdater.builder();
    }

    @Test
    public void addFileToBlankVersion() {
        var updater = builder.buildBlankState(inventory);

        var result = updater.addFile("file3", "file3p");

        assertAddResult("file3p", result);
    }

    @Test
    public void addFileToMutableBlankVersion() {
        var updater = builder.buildCopyStateMutable(inventory);

        var result = updater.addFile("file3", "file3p");

        assertTrue(result.isNew(), "isNew");
        assertEquals(OcflConstants.MUTABLE_HEAD_VERSION_PATH + "/content/r1/file3p", result.getContentPath());
        assertEquals("r1/file3p", result.getPathUnderContentDir());
    }

    @Test
    public void removeFileFromMutableVersion() {
        var updater = builder.buildCopyStateMutable(inventory);

        updater.addFile("file3", "file3p");
        var results = updater.removeFile("file3p");

        assertEquals(1, results.size());
        var result = results.iterator().next();

        assertEquals(OcflConstants.MUTABLE_HEAD_VERSION_PATH + "/content/r1/file3p", result.getContentPath());
        assertEquals("r1/file3p", result.getPathUnderContentDir());
    }

    @Test
    public void addFileToBlankVersionWhenFileIdAlreadyExists() {
        var updater = builder.buildBlankState(inventory);

        var result = updater.addFile("file1", "file3p");

        assertFalse(result.isNew(), "isNew");
    }

    @Test
    public void addFileShouldFailWhenTheresAlreadyAFileAtTheLogicalPathAndNoOverwrite() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(OverwriteException.class, "There is already a file at", () -> {
            updater.addFile("file3", "file1p");
        });
    }

    @Test
    public void addFileShouldOverwriteWhenTheresAlreadyAFileAtTheLogicalPathAndOverwrite() {
        var updater = builder.buildCopyState(inventory);

        var result = updater.addFile("file3", "file1p", OcflOption.OVERWRITE);

        assertAddResult("file1p", result);
    }

    @Test
    public void shouldRemoveFileWhenExists() {
        var updater = builder.buildCopyState(inventory);

        var result = updater.removeFile("file1p");

        assertEquals(Collections.emptySet(), result);
    }

    @Test
    public void shouldRemoveFileFromManifestWhenAddedInSameVersion() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");
        var result = updater.removeFile("file3p");

        assertEquals(1, result.size());
        assertRemoveResult("file3p", result.iterator().next());
    }

    @Test
    public void shouldRenameFileWhenSrcExistsAndDstDoesNot() {
        var updater = builder.buildCopyState(inventory);

        var result = updater.renameFile("file1p", "file3p");

        assertEquals(0, result.size());
    }

    @Test
    public void shouldFailRenameWhenSrcDoesNotExist() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "path was not found in object", () -> {
            updater.renameFile("file2p", "file3p");
        });
    }

    @Test
    public void shouldFailRenameWhenSrcExistsAndDstExistNoOverwrite() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");

        OcflAsserts.assertThrowsWithMessage(OverwriteException.class, "There is already a file at", () -> {
            updater.renameFile("file1p", "file3p");
        });
    }

    @Test
    public void shouldRenameWhenSrcExistsAndDstExistAndOverwrite() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");
        var results = updater.renameFile("file1p", "file3p", OcflOption.OVERWRITE);

        assertEquals(1, results.size());
        assertRemoveResult("file3p", results.iterator().next());
    }

    @Test
    public void shouldReinstateFileWhenSrcExistsAndDstNotExists() {
        var updater = builder.buildCopyState(inventory);

        var results = updater.reinstateFile(VersionId.fromString("v1"), "file2p", "file3p");

        assertEquals(0, results.size());
    }

    @Test
    public void shouldFailReinstateFileWhenSrcVersionNotExists() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "does not contain a file at", () -> {
            updater.reinstateFile(VersionId.fromString("v4"), "file2p", "file3p");
        });
    }

    @Test
    public void shouldFailReinstateFileWhenSrcFileNotExists() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "does not contain a file at", () -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file4p", "file3p");
        });
    }

    @Test
    public void shouldFailReinstateFileWhenDstExistsNoOverwrite() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(OverwriteException.class, "There is already a file at", () -> {
            updater.reinstateFile(VersionId.fromString("v1"), "file2p", "file1p");
        });
    }

    @Test
    public void shouldReinstateFileWhenDstExistsWithOverwrite() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");
        var results = updater.reinstateFile(VersionId.fromString("v1"), "file2p", "file3p", OcflOption.OVERWRITE);

        assertEquals(1, results.size());
        assertRemoveResult("file3p", results.iterator().next());
    }

    @Test
    public void shouldRejectInvalidLogicalPaths() {
        var updater = builder.buildCopyState(inventory);

        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "illegal empty filename", () -> {
            updater.addFile("id", "path//file");
        });
        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "invalid sequence", () -> {
            updater.addFile("id", "path/../blah");
        });
        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "invalid sequence", () -> {
            updater.addFile("id", "./blah");
        });

        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "invalid sequence", () -> {
            updater.renameFile("path","./blah");
        });
        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "invalid sequence", () -> {
            updater.reinstateFile(VersionId.fromString("v1"), "path","./blah");
        });

        assertDoesNotThrow(() -> {
            updater.removeFile("./blah");
        });
    }

    @Test
    public void shouldAddFixityWhenFileInVersionAndNotDefaultAlgorithm() {
        var updater = builder.buildCopyState(inventory);

        updater.addFixity("file1p", DigestAlgorithm.md5, "md5_1");

        assertEquals("md5_1", updater.getFixityDigest("file1p", DigestAlgorithm.md5));
    }

    @Test
    public void shouldReturnNothingWhenWrongAlgorithm() {
        var updater = builder.buildCopyState(inventory);

        updater.addFixity("file1p", DigestAlgorithm.md5, "md5_1");

        assertNull(updater.getFixityDigest("file1p", DigestAlgorithm.sha1));
    }

    @Test
    public void shouldNotAddFixityWhenDefaultAlgorithm() {
        var updater = builder.buildCopyState(inventory);

        updater.addFixity("file1p", DigestAlgorithm.sha512, "sha512_1");

        assertEquals("file1", updater.getFixityDigest("file1p", DigestAlgorithm.sha512));
    }

    @Test
    public void shouldNotAddFixityWhenFileNotInState() {
        var updater = builder.buildCopyState(inventory);

        updater.addFixity("file2p", DigestAlgorithm.md5, "md5_1");

        assertNull(updater.getFixityDigest("file2p", DigestAlgorithm.md5));
    }

    @Test
    public void shouldClearFixity() {
        var updater = builder.buildCopyState(inventory);

        updater.addFixity("file1p", DigestAlgorithm.md5, "md5_1");
        updater.clearFixity();

        assertNull(updater.getFixityDigest("file1p", DigestAlgorithm.md5));
    }

    @Test
    public void shouldClearState() {
        var updater = builder.buildCopyState(inventory);

        updater.clearState();

        var inventory = updater.buildNewInventory(OffsetDateTime.now(), null);
        assertEquals(0, inventory.getHeadVersion().getState().size());
    }

    private void assertAddResult(String expectedLogical, InventoryUpdater.AddFileResult result) {
        assertTrue(result.isNew(), "isNew");
        assertEquals("v3/content/" + expectedLogical, result.getContentPath());
        assertEquals(expectedLogical, result.getPathUnderContentDir());
    }

    private void assertRemoveResult(String logicalPath, InventoryUpdater.RemoveFileResult result) {
        assertEquals("v3/content/" + logicalPath, result.getContentPath());
        assertEquals(logicalPath, result.getPathUnderContentDir());
    }

}
