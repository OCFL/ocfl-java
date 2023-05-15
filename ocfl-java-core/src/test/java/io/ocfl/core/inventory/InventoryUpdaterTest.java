package io.ocfl.core.inventory;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.ocfl.api.OcflConfig;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.OcflOption;
import io.ocfl.api.exception.OcflInputException;
import io.ocfl.api.exception.OverwriteException;
import io.ocfl.api.exception.PathConstraintException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.Version;
import java.time.OffsetDateTime;
import java.util.Collections;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class InventoryUpdaterTest {

    private Inventory inventory;
    private InventoryUpdater.Builder builder;

    @BeforeEach
    public void setup() {
        inventory = Inventory.stubInventory(
                        "id", new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "root")
                .buildFrom()
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

        assertThatThrownBy(() -> {
                    updater.addFile("file3", "file1p");
                })
                .isInstanceOf(OverwriteException.class)
                .hasMessageContaining("There is already a file at");
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

        assertThatThrownBy(() -> {
                    updater.renameFile("file2p", "file3p");
                })
                .isInstanceOf(OcflInputException.class)
                .hasMessageContaining("path was not found in object");
    }

    @Test
    public void shouldFailRenameWhenSrcExistsAndDstExistNoOverwrite() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");

        assertThatThrownBy(() -> {
                    updater.renameFile("file1p", "file3p");
                })
                .isInstanceOf(OverwriteException.class)
                .hasMessageContaining("There is already a file at");
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

        var results = updater.reinstateFile(VersionNum.fromString("v1"), "file2p", "file3p");

        assertEquals(0, results.size());
    }

    @Test
    public void shouldFailReinstateFileWhenSrcVersionNotExists() {
        var updater = builder.buildCopyState(inventory);

        assertThatThrownBy(() -> {
                    updater.reinstateFile(VersionNum.fromString("v4"), "file2p", "file3p");
                })
                .isInstanceOf(OcflInputException.class)
                .hasMessageContaining("does not contain a file at");
    }

    @Test
    public void shouldFailReinstateFileWhenSrcFileNotExists() {
        var updater = builder.buildCopyState(inventory);

        assertThatThrownBy(() -> {
                    updater.reinstateFile(VersionNum.fromString("v1"), "file4p", "file3p");
                })
                .isInstanceOf(OcflInputException.class)
                .hasMessageContaining("does not contain a file at");
    }

    @Test
    public void shouldFailReinstateFileWhenDstExistsNoOverwrite() {
        var updater = builder.buildCopyState(inventory);

        assertThatThrownBy(() -> {
                    updater.reinstateFile(VersionNum.fromString("v1"), "file2p", "file1p");
                })
                .isInstanceOf(OverwriteException.class)
                .hasMessageContaining("There is already a file at");
    }

    @Test
    public void shouldReinstateFileWhenDstExistsWithOverwrite() {
        var updater = builder.buildCopyState(inventory);

        updater.addFile("file3", "file3p");
        var results = updater.reinstateFile(VersionNum.fromString("v1"), "file2p", "file3p", OcflOption.OVERWRITE);

        assertEquals(1, results.size());
        assertRemoveResult("file3p", results.iterator().next());
    }

    @Test
    public void shouldRejectInvalidLogicalPaths() {
        var updater = builder.buildCopyState(inventory);

        assertThatThrownBy(() -> {
                    updater.addFile("id", "path//file");
                })
                .isInstanceOf(PathConstraintException.class)
                .hasMessageContaining("illegal empty filename");

        assertThatThrownBy(() -> {
                    updater.addFile("id", "path/../blah");
                })
                .isInstanceOf(PathConstraintException.class)
                .hasMessageContaining("invalid sequence");

        assertThatThrownBy(() -> {
                    updater.addFile("id", "./blah");
                })
                .isInstanceOf(PathConstraintException.class)
                .hasMessageContaining("invalid sequence");

        assertThatThrownBy(() -> {
                    updater.renameFile("path", "./blah");
                })
                .isInstanceOf(PathConstraintException.class)
                .hasMessageContaining("invalid sequence");

        assertThatThrownBy(() -> {
                    updater.reinstateFile(VersionNum.fromString("v1"), "path", "./blah");
                })
                .isInstanceOf(PathConstraintException.class)
                .hasMessageContaining("invalid sequence");

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
