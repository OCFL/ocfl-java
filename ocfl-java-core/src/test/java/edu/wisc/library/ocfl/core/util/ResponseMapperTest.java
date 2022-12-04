package edu.wisc.library.ocfl.core.util;

import static edu.wisc.library.ocfl.test.matcher.OcflMatchers.fileChange;
import static edu.wisc.library.ocfl.test.matcher.OcflMatchers.versionInfo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import java.time.OffsetDateTime;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ResponseMapperTest {

    private ResponseMapper responseMapper;

    @BeforeEach
    public void setup() {
        this.responseMapper = new ResponseMapper();
    }

    @Test
    public void shouldIncludeSingleChangeWhenFileAddedAndNotUpdated() {
        var inventory = Inventory.stubInventory(
                        "o1", new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "o1")
                .buildFrom()
                .addFileToManifest("i1", "v1/content/f1")
                .addFileToManifest("i2", "v2/content/f2")
                .addFileToManifest("i3", "v3/content/f3")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .addFile("i2", "f2")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .addFile("i3", "f3")
                        .build())
                .build();

        var history = responseMapper.fileChangeHistory(inventory, "f1");

        assertThat(
                history.getFileChanges(),
                contains(fileChange(
                        FileChangeType.UPDATE,
                        ObjectVersionId.version("o1", "v1"),
                        "f1",
                        "o1/v1/content/f1",
                        versionInfo(null, null, null),
                        Map.of(DigestAlgorithm.sha512, "i1"))));
    }

    @Test
    public void shouldIncludeRemoveChangeWhenFileAddedAndRemoved() {
        var inventory = Inventory.stubInventory(
                        "o1", new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "o1")
                .buildFrom()
                .addFileToManifest("i1", "v1/content/f1")
                .addFileToManifest("i2", "v2/content/f2")
                .addFileToManifest("i3", "v3/content/f3")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .addFile("i2", "f2")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i3", "f3")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .addFile("i3", "f3")
                        .build())
                .build();

        var history = responseMapper.fileChangeHistory(inventory, "f1");

        assertThat(
                history.getFileChanges(),
                contains(
                        fileChange(
                                FileChangeType.UPDATE,
                                ObjectVersionId.version("o1", "v1"),
                                "f1",
                                "o1/v1/content/f1",
                                versionInfo(null, null, null),
                                Map.of(DigestAlgorithm.sha512, "i1")),
                        fileChange(
                                FileChangeType.REMOVE,
                                ObjectVersionId.version("o1", "v3"),
                                "f1",
                                null,
                                versionInfo(null, null, null),
                                Map.of()),
                        fileChange(
                                FileChangeType.UPDATE,
                                ObjectVersionId.version("o1", "v4"),
                                "f1",
                                "o1/v1/content/f1",
                                versionInfo(null, null, null),
                                Map.of(DigestAlgorithm.sha512, "i1"))));
    }

    @Test
    public void shouldIncludeMultipleUpdatesWhenContentChangs() {
        var inventory = Inventory.stubInventory(
                        "o1", new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "o1")
                .buildFrom()
                .addFileToManifest("i1", "v1/content/f1")
                .addFileToManifest("i2", "v2/content/f1")
                .addFileToManifest("i3", "v3/content/f1")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i1", "f1")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i2", "f1")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("i3", "f1")
                        .build())
                .build();

        var history = responseMapper.fileChangeHistory(inventory, "f1");

        assertThat(
                history.getFileChanges(),
                contains(
                        fileChange(
                                FileChangeType.UPDATE,
                                ObjectVersionId.version("o1", "v1"),
                                "f1",
                                "o1/v1/content/f1",
                                versionInfo(null, null, null),
                                Map.of(DigestAlgorithm.sha512, "i1")),
                        fileChange(
                                FileChangeType.UPDATE,
                                ObjectVersionId.version("o1", "v2"),
                                "f1",
                                "o1/v2/content/f1",
                                versionInfo(null, null, null),
                                Map.of(DigestAlgorithm.sha512, "i2")),
                        fileChange(
                                FileChangeType.UPDATE,
                                ObjectVersionId.version("o1", "v3"),
                                "f1",
                                "o1/v3/content/f1",
                                versionInfo(null, null, null),
                                Map.of(DigestAlgorithm.sha512, "i3"))));
    }
}
