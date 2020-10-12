package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.core.model.*;
import edu.wisc.library.ocfl.core.validation.InventoryValidator;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.util.HashMap;

public class InventoryValidatorTest {

    private InventoryBuilder stubBuilder;

    @BeforeEach
    public void setup() {
        this.stubBuilder = Inventory.builder(Inventory.stubInventory("o1", new OcflConfig(), "root"))
                .head(new VersionId(1));
    }

    @Test
    public void failWhenHeadV0() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "HEAD version must be greater than v0", () -> {
            InventoryValidator.validateShallow(stubBuilder.head(new VersionId(0)).build());
        });
    }

    @Test
    public void failWhenContentDirHasSlashes() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Content directory cannot contain", () -> {
            InventoryValidator.validateShallow(stubBuilder.contentDirectory("path/dir").build());
        });
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Content directory cannot contain", () -> {
            InventoryValidator.validateShallow(stubBuilder.contentDirectory("path\\dir").build());
        });
    }

    @Test
    public void failWhenContentDirBlank() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Content directory cannot be blank", () -> {
            InventoryValidator.validateShallow(stubBuilder.contentDirectory("").build());
        });
    }

    @Test
    public void failWhenFixityReferencesContentPathNotInManifest() {
        var fixityMap = new HashMap<DigestAlgorithm, PathBiMap>();
        var fixity = new PathBiMap();
        fixity.put("md5_123", "contentPath");
        fixityMap.put(DigestAlgorithm.md5, fixity);

        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Fixity entry md5 => {md5_123 => contentPath} does not have a corresponding entry in the manifest block.", () -> {
            InventoryValidator.validateDeep(stubBuilder.fixityBiMap(fixityMap).build());
        });
    }

    @Test
    public void failWhenVersionsEmpty() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Versions cannot be empty", () -> {
            InventoryValidator.validateShallow(stubBuilder.build());
        });
    }

    @Test
    public void failWhenUserNameBlank() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "User name in version v1 cannot be blank", () -> {
            InventoryValidator.validateShallow(stubBuilder
                    .head(new VersionId(0))
                    .addHeadVersion(Version.builder()
                            .created(OffsetDateTime.now())
                            .user(new User(null, null))
                            .build()).build());
        });
    }

    @Test
    public void failWhenMissingVersion() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Version v1 is missing", () -> {
            InventoryValidator.validateShallow(stubBuilder
                    .head(new VersionId(1))
                    .addHeadVersion(Version.builder()
                            .created(OffsetDateTime.now())
                            .build()).build());
        });
    }

    @Test
    public void failWhenFileNotReferencedInManifest() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "Version state entry 1 => [file1] in version v1 does not have a corresponding entry in the manifest block.", () -> {
            InventoryValidator.validateShallow(stubBuilder
                    .head(new VersionId(0))
                    .addHeadVersion(Version.builder()
                            .created(OffsetDateTime.now())
                            .addFile("1", "file1")
                            .build()).build());
        });
    }

    @Test
    public void failWhenHeadVersionNotLatestVersion() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "HEAD must be the latest version. Expected: v2; Was: v1", () -> {
            InventoryValidator.validateShallow(stubBuilder
                    .head(new VersionId(0))
                    .addHeadVersion(Version.builder()
                            .created(OffsetDateTime.now())
                            .build())
                    .addHeadVersion(Version.builder()
                            .created(OffsetDateTime.now())
                            .build())
                    .head(new VersionId(1)).build());
        });
    }

}
