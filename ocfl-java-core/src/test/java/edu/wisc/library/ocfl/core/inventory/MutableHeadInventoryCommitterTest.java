package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.OcflConfig;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.model.Version;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;

import static org.junit.jupiter.api.Assertions.*;

public class MutableHeadInventoryCommitterTest {

    private MutableHeadInventoryCommitter committer;

    @BeforeEach
    public void setup() {
        committer = new MutableHeadInventoryCommitter();
    }

    @Test
    public void shouldRewriteInventoryWhenHasMutableContents() {
        var date1 = OffsetDateTime.now().minusDays(3);
        var date2 = OffsetDateTime.now().minusDays(2);
        var date3 = OffsetDateTime.now().minusDays(1);
        var now = OffsetDateTime.now();

        var original = Inventory.builder(Inventory.stubInventory("o1", new OcflConfig(), "root"))
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
                .revisionId(RevisionId.fromString("r3"))
                .build();


        var newInventory = committer.commit(original, now, new CommitInfo().setMessage("commit"));

        assertEquals(VersionId.fromString("v3"), newInventory.getHead());
        assertNull(newInventory.getRevisionId(), "revisionId");
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
