package io.ocfl.core.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import io.ocfl.api.OcflConfig;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.OcflVersion;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.RevisionNum;
import io.ocfl.core.model.Version;
import java.time.OffsetDateTime;
import org.junit.jupiter.api.Test;

public class MutableHeadInventoryCommitterTest {

    private OcflConfig config = new OcflConfig().setOcflVersion(OcflVersion.OCFL_1_1);

    @Test
    public void shouldRewriteInventoryWhenHasMutableContents() {
        var date1 = OffsetDateTime.now().minusDays(3);
        var date2 = OffsetDateTime.now().minusDays(2);
        var date3 = OffsetDateTime.now().minusDays(1);
        var now = OffsetDateTime.now();

        var original = Inventory.stubInventory(
                        "o1", new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "root")
                .buildFrom()
                .addFileToManifest("f1", "v1/content/file1")
                .addFileToManifest("f2", "v1/content/file2")
                .addFileToManifest("f3", "v2/content/file3")
                .addFileToManifest("f4", mutableContentPath("r1/file4"))
                .addFileToManifest("f5", mutableContentPath("r3/file5"))
                .addFixityForFile("v1/content/file1", DigestAlgorithm.md5, "md5_1")
                .addFixityForFile(mutableContentPath("r1/file4"), DigestAlgorithm.md5, "md5_4")
                .addHeadVersion(Version.builder()
                        .addFile("f1", "file1")
                        .addFile("f2", "file2")
                        .created(date1)
                        .message("1")
                        .build())
                .addHeadVersion(Version.builder()
                        .addFile("f1", "file2")
                        .addFile("f3", "file3")
                        .created(date2)
                        .message("2")
                        .build())
                .addHeadVersion(Version.builder()
                        .addFile("f1", "file3")
                        .addFile("f4", "file4")
                        .addFile("f5", "file5")
                        .created(date3)
                        .message("3")
                        .build())
                .mutableHead(true)
                .revisionNum(RevisionNum.fromString("r3"))
                .build();

        var newInventory =
                MutableHeadInventoryCommitter.commit(original, now, new VersionInfo().setMessage("commit"), config);

        assertEquals(VersionNum.fromString("v3"), newInventory.getHead());
        assertNull(newInventory.getRevisionNum(), "revisionNum");
        assertFalse(newInventory.hasMutableHead(), "mutableHead");

        assertEquals("v1/content/file1", newInventory.getContentPath("f1"));
        assertEquals("v1/content/file2", newInventory.getContentPath("f2"));
        assertEquals("v2/content/file3", newInventory.getContentPath("f3"));
        assertEquals("v3/content/r1/file4", newInventory.getContentPath("f4"));
        assertEquals("v3/content/r3/file5", newInventory.getContentPath("f5"));

        var fixity = newInventory.getFixityForContentPath("v3/content/r1/file4");

        assertEquals(1, fixity.size());
        assertEquals("md5_4", fixity.get(DigestAlgorithm.md5));

        var version = newInventory.getHeadVersion();
        assertSame(now, version.getCreated());
        assertEquals("commit", version.getMessage());
        assertEquals("f1", version.getFileId("file3"));
        assertEquals("f4", version.getFileId("file4"));
        assertEquals("f5", version.getFileId("file5"));
    }

    private String mutableContentPath(String suffix) {
        return OcflConstants.MUTABLE_HEAD_VERSION_PATH + "/content/" + suffix;
    }
}
