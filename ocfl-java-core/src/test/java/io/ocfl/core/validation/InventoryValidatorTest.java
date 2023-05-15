package io.ocfl.core.validation;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import io.ocfl.api.OcflConfig;
import io.ocfl.api.OcflConstants;
import io.ocfl.api.exception.InvalidInventoryException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.VersionNum;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.InventoryBuilder;
import io.ocfl.core.model.User;
import io.ocfl.core.model.Version;
import java.time.OffsetDateTime;

import org.junit.jupiter.api.Test;

public class InventoryValidatorTest {

    @Test
    public void failWhenHeadV0() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder().build());
                },
                "HEAD version must be greater than v0");
    }

    @Test
    public void failWhenContentDirHasSlashes() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .head(new VersionNum(1))
                            .contentDirectory("path/dir")
                            .build());
                },
                "Content directory cannot contain");
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .head(new VersionNum(1))
                            .contentDirectory("path\\dir")
                            .build());
                },
                "Content directory cannot contain");
    }

    @Test
    public void failWhenContentDirBlank() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .head(new VersionNum(1))
                            .contentDirectory("")
                            .build());
                },
                "Content directory cannot be blank");
    }

    @Test
    public void failWhenVersionsEmpty() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(
                            defaultBuilder().head(new VersionNum(1)).build());
                },
                "Versions cannot be empty");
    }

    @Test
    public void failWhenUserNameBlank() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .addHeadVersion(Version.builder()
                                    .created(OffsetDateTime.now())
                                    .user(new User(null, null))
                                    .build())
                            .build());
                },
                "User name in version v1 cannot be blank");
    }

    @Test
    public void failWhenMissingVersion() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .head(new VersionNum(1))
                            .addHeadVersion(Version.builder()
                                    .created(OffsetDateTime.now())
                                    .build())
                            .build());
                },
                "Version v1 is missing");
    }

    @Test
    public void failWhenFileNotReferencedInManifest() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .addHeadVersion(Version.builder()
                                    .created(OffsetDateTime.now())
                                    .addFile("1", "file1")
                                    .build())
                            .build());
                },
                "Version state entry 1 => [file1] in version v1 does not have a corresponding entry in the manifest block.");
    }

    @Test
    public void failWhenHeadVersionNotLatestVersion() {
        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateShallow(defaultBuilder()
                            .addHeadVersion(Version.builder()
                                    .created(OffsetDateTime.now())
                                    .build())
                            .addHeadVersion(Version.builder()
                                    .created(OffsetDateTime.now())
                                    .build())
                            .head(new VersionNum(1))
                            .build());
                },
                "HEAD must be the latest version. Expected: v2; Was: v1");
    }

    @Test
    public void failWhenVersionNumberPaddedDifferently() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .head(VersionNum.fromString("v000"))
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateVersionStates(currentInventory, previousInventory);
                },
                "Inventory o1 v2: Version number formatting differs: v001 vs v1");
    }

    @Test
    public void failWhenVersionStatesDoNotMatch() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("def456", "file2")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateVersionStates(currentInventory, previousInventory);
                },
                "In object o1 the inventories in version v2 and v1 define a different state for version v1.");
    }

    @Test
    public void failWhenVersionStatesDoNotMatchAndOneValid() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .addFile("abc789", "file3")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .head(new VersionNum(0))
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .build())
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .addFile("abc789", "file3")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateVersionStates(currentInventory, previousInventory);
                },
                "In object o1 the inventories in version v3 and v2 define a different state for version v1.");
    }

    @Test
    public void doNotFailWhenUserChanged() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .user(new User("test", "test@example.com"))
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .user(new User("example", "example@test.com"))
                        .build())
                .build();

        InventoryValidator.validateVersionStates(currentInventory, previousInventory);
    }

    @Test
    public void failWhenIdDifferent() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = builder("o2")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateCompatibleInventories(currentInventory, previousInventory);
                },
                "Object IDs are not the same. Existing: o2; New: o1");
    }

    @Test
    public void failWhenAlgorithmsDifferent() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .digestAlgorithm(DigestAlgorithm.sha256)
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateCompatibleInventories(currentInventory, previousInventory);
                },
                "Inventory digest algorithms are not the same. Existing: sha256; New: sha512");
    }

    @Test
    public void failWhenContentDirectoryDifferent() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .contentDirectory("new-content")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateCompatibleInventories(currentInventory, previousInventory);
                },
                "Inventory content directories are not the same. Existing: new-content; New: content");
    }

    @Test
    public void failWhenNotOneVersionApart() {
        var currentInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .addHeadVersion(Version.builder().created(OffsetDateTime.now()).build())
                .build();

        var previousInventory = defaultBuilder()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("abc123", "file1")
                        .addFile("abc456", "file2")
                        .build())
                .build();

        expectInvalidInventory(
                () -> {
                    InventoryValidator.validateCompatibleInventories(currentInventory, previousInventory);
                },
                "The new HEAD inventory version must be the next sequential version number. Existing: v1; New: v3");
    }

    private InventoryBuilder defaultBuilder() {
        return builder("o1");
    }

    private InventoryBuilder builder(String id) {
        return Inventory.stubInventory(id, new OcflConfig().setOcflVersion(OcflConstants.DEFAULT_OCFL_VERSION), "root")
                .buildFrom();
    }

    private void expectInvalidInventory(Runnable r, String message) {
        assertThatThrownBy(r::run).isInstanceOf(InvalidInventoryException.class).hasMessageContaining(message);
    }
}
