package io.ocfl.core.util;

import static org.assertj.core.api.Assertions.assertThat;

import io.ocfl.api.DigestAlgorithmRegistry;
import io.ocfl.api.OcflConfig;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.model.FileChange;
import io.ocfl.api.model.FileChangeType;
import io.ocfl.api.model.ObjectVersionId;
import io.ocfl.api.model.VersionInfo;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.Version;
import java.time.OffsetDateTime;
import java.util.List;
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

        assertFileChanges(
                history.getFileChanges(),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v1"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v1/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i1")));
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

        assertFileChanges(
                history.getFileChanges(),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v1"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v1/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i1")),
                new FileChange()
                        .setChangeType(FileChangeType.REMOVE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v3"))
                        .setPath("f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of()),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v4"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v1/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i1")));
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

        assertFileChanges(
                history.getFileChanges(),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v1"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v1/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i1")),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v2"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v2/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i2")),
                new FileChange()
                        .setChangeType(FileChangeType.UPDATE)
                        .setObjectVersionId(ObjectVersionId.version("o1", "v3"))
                        .setPath("f1")
                        .setStorageRelativePath("o1/v3/content/f1")
                        .setVersionInfo(new VersionInfo())
                        .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "i3")));
    }

    private void assertFileChanges(List<FileChange> actual, FileChange... expected) {
        assertThat(actual)
                .usingRecursiveFieldByFieldElementComparatorIgnoringFields("timestamp", "versionInfo.created")
                .containsExactly(expected);
    }
}
