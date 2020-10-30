package edu.wisc.library.ocfl.core.validation;

import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.nio.file.Paths;

public class ObjectValidatorTest {

    private ObjectValidator validator;
    private InventoryMapper inventoryMapper;

    @BeforeEach
    public void setup() {
        inventoryMapper = InventoryMapper.defaultMapper();
        validator = new ObjectValidator(inventoryMapper);
    }

    @Test
    public void failWhenNamasteFileDoesNotExist() {
        validateObject("no-namaste",
                replaceSlashes("Expected file src/test/resources/validation/object/no-namaste/0=ocfl_object_1.0 to exist, but it does not."));
    }

    @Test
    public void failWhenMissingRootSidecar() {
        validateObject("no-root-sidecar",
                replaceSlashes("Expected there to be one inventory sidecar file in src/test/resources/validation/object/no-root-sidecar, but found 0."));
    }

    @Test
    public void failWhenRootInventoryDigestDoesNotMatchExpectation() {
        validateObject("invalid-root-inventory",
                replaceSlashes("Inventory file at src/test/resources/validation/object/invalid-root-inventory/inventory.json does not match expected sha512 digest: Expected ab0fbc38aff2a52b53d07a069082d4a16fca6f1796430bfb260facba06d242f5b336009801023f9f0d21b88b4ab609604e516594fa4cc0c554af566e87b936fc; Actual 570fbc38aff2a52b53d07a069082d4a16fca6f1796430bfb260facba06d242f5b336009801023f9f0d21b88b4ab609604e516594fa4cc0c554af566e87b936fc"));
    }

    @Test
    public void failWhenVersionInventoryMissing() {
        validateObject("no-version-inventory",
                replaceSlashes("Expected file src/test/resources/validation/object/no-version-inventory/v1/inventory.json to exist, but it does not."));
    }

    @Test
    public void failWhenVersionSidecarMissing() {
        validateObject("no-version-sidecar",
                replaceSlashes("Expected there to be one inventory sidecar file in src/test/resources/validation/object/no-version-sidecar/v1, but found 0."));
    }

    @Test
    public void failWhenVersionInventoryDigestDoesNotMatchExpectation() {
        validateObject("invalid-version-inventory",
                replaceSlashes("Inventory file at src/test/resources/validation/object/invalid-version-inventory/v1/inventory.json does not match expected sha512 digest: Expected 12cbceab0da055d8bab0862462fb3f712d94e4c3eb40dedd7368f8e3a28d63f5620e2c3bf6936fda2ee4dffb10f6c0df86827727a732fc97b590734382344cd4; Actual ebcbceab0da055d8bab0862462fb3f712d94e4c3eb40dedd7368f8e3a28d63f5620e2c3bf6936fda2ee4dffb10f6c0df86827727a732fc97b590734382344cd4"));
    }

    @Test
    public void failWhenVersionRootContainsUnexpectedFile() {
        validateObject("version-root-unexpected-file",
                replaceSlashes("Version contains an illegal file at src/test/resources/validation/object/version-root-unexpected-file/v1/bad-file"));
    }

    @Test
    public void failWhenUnexpectedFileInRoot() {
        validateObject("object-root-unexpected-file",
                replaceSlashes("Object o1 contains an invalid file: src/test/resources/validation/object/object-root-unexpected-file/bad-file"));
    }

    @Test
    public void failWhenUnexpectedVersionInRoot() {
        validateObject("object-root-unexpected-version",
                replaceSlashes("Object o1 contains an invalid file: src/test/resources/validation/object/object-root-unexpected-version/v3"));
    }

    @Test
    public void failWhenRootInventoryNotSameAsHeadVersionInventory() {
        validateObject("object-root-inventory-head-mismatch",
                "The inventory file in the object root of object o1 does not match the inventory in the version directory v2");
    }

    @Test
    public void failWhenManifestContainsFileNotOnDisk() {
        validateObject("file-missing-on-disk",
                "The following files are defined in object o1's manifest, but are not found on disk: {4cf0ff5673ec65d9900df95502ed92b2605fc602ca20b6901652c7561b302668026095813af6adb0e663bdcdbe1f276d18bf0de254992a78573ad6574e7ae1f6=[v1/content/file2]}");
    }

    @Test
    public void failWhenFileOnDiskNotFoundInManifest() {
        validateObject("file-missing-in-manifest",
                replaceSlashes("Object o1 contains an unexpected file: src/test/resources/validation/object/file-missing-in-manifest/v1/content/bonus-file"));
    }

    @Test
    public void failWhenFileOnDiskDoesNotMatchPathInManifest() {
        validateObject("file-mismatch",
                replaceSlashes("File src/test/resources/validation/object/file-mismatch/v1/content/file2 has unexpected sha512 digest value 96a26e7629b55187f9ba3edc4acc940495d582093b8a88cb1f0303cf3399fe6b1f5283d76dfd561fc401a0cdf878c5aad9f2d6e7e2d9ceee678757bb5d95c39e."));
    }

    @Test
    public void failWhenObjectIdsDoNotMatch() {
        validateObject("object-id-mismatch",
                "Versions v2 and v1 of object o1 have different object IDs, o1 and o1.a.");
    }

    @Test
    public void failWhenVersionNumberPaddingDifferent() {
        validateObject("padding-mismatch",
                replaceSlashes("Expected version v1 but was v0001 in src/test/resources/validation/object/padding-mismatch/v1/inventory.json."));
    }

    @Test
    public void failWhenVersionStatesDifferent() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "In object o1 the inventories in version v2 and v1 define a different state for version v1.", () -> {
            validateObject("state-mismatch");
        });
    }

    @Test
    public void failWhenVersionStatesDifferentAndDigestChanged() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "In object o1 the inventories in version v2 and v1 define a different state for version v1.", () -> {
            validateObject("state-mismatch-different-digest");
        });
    }

    @Test
    public void validateWhenVersionStatesDigestChangedButStillValid() {
        validateObject("different-digest");
    }

    @Test
    public void failVersionWhenSidecarMissing() {
        validateVersion("missing-sidecar",
                replaceSlashes("Expected there to be one inventory sidecar file in src/test/resources/validation/version/missing-sidecar, but found 0."));
    }

    @Test
    public void failVersionWhenInventoryDigestWrong() {
        validateVersion("inventory-digest-wrong",
                replaceSlashes("Inventory file at src/test/resources/validation/version/inventory-digest-wrong/inventory.json does not match expected sha512 digest: Expected ab0fbc38aff2a52b53d07a069082d4a16fca6f1796430bfb260facba06d242f5b336009801023f9f0d21b88b4ab609604e516594fa4cc0c554af566e87b936fc; Actual 570fbc38aff2a52b53d07a069082d4a16fca6f1796430bfb260facba06d242f5b336009801023f9f0d21b88b4ab609604e516594fa4cc0c554af566e87b936fc"));
    }

    @Test
    public void failVersionWhenContainsIllegalFiles() {
        validateVersion("illegal-files",
                replaceSlashes("Version contains an illegal file at src/test/resources/validation/version/illegal-files/bad-file"));
    }

    @Test
    public void failVersionWhenContentContainsUnexpectedFile() {
        validateVersion("illegal-content-files",
                replaceSlashes("Object o1 contains an unexpected file: src/test/resources/validation/version/illegal-content-files/content/bad-file"));
    }

    @Test
    public void failVersionWhenContentFileDigestDoesNotMatch() {
        validateVersion("content-digest-mismatch",
                replaceSlashes("File src/test/resources/validation/version/content-digest-mismatch/content/file1 has unexpected sha512 digest value. Expected: aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc; Actual: 9d0d50239dc4de7bf863f296376cc9d6d6904e5867f7f79a86451c7b13dda0e8e702141141c109852c79e9bc738a32e6f1a81a035efb0b45c830a721095f63b9."));
    }

    @Test
    public void failVersionWhenMissingFiles() {
        validateVersion("content-file-missing",
                "The following files are defined in object o1's manifest, but are not found on disk: {aff2318b35d3fbc05670b834b9770fd418e4e1b4adc502e6875d598ab3072ca76667121dac04b694c47c71be80f6d259316c7bd0e19d40827cb3f27ee03aa2fc=[v2/content/file1]}");
    }

    @Test
    public void failVersionWhenMissingFilesWhenMultipleAssociatedToSameDigest() {
        validateVersion("content-file-missing-when-duplicate",
                "The following files are defined in object o1's manifest, but are not found on disk: {cb6f4f7b3d3eef05d3d0327335071d14c120e065fa43364690fea47d456e146dd334d78d35f73926067d0bf46f122ea026508954b71e8e25c351ff75c993c2b2=[v2/content/dir1/file4]}");
    }

    @Test
    public void validateVersionWhenMultipleFilesAssociatedToDigestInDifferentVersions() {
        validateVersion("content-file-missing-when-duplicate-different-versions");
    }

    @Test
    public void validateVersionWhenHasNoContent() {
        validateVersion("no-content");
    }

    @Test
    public void failVersionWhenHasPathConflict() {
        OcflAsserts.assertThrowsWithMessage(InvalidInventoryException.class, "In version v1 the logical path file1 conflicts with another logical path.", () -> {
            validateObject("path-conflict");
        });
    }

    private void validateObject(String name) {
        var objectPath = Paths.get("src/test/resources/validation/object", name);
        var inventory = inventoryMapper.read(objectPath.toString(), "digest", ObjectPaths.inventoryPath(objectPath));
        validator.validateObject(objectPath, inventory);
    }

    private void validateObject(String name, String exceptionMessage) {
        OcflAsserts.assertThrowsWithMessage(CorruptObjectException.class, exceptionMessage, () -> {
            validateObject(name);
        });
    }

    private void validateVersion(String name) {
        var versionPath = Paths.get("src/test/resources/validation/version", name);
        var inventory = inventoryMapper.read(versionPath.toString(), "digest", ObjectPaths.inventoryPath(versionPath));
        validator.validateVersion(versionPath, inventory);
    }

    private void validateVersion(String name, String exceptionMessage) {
        OcflAsserts.assertThrowsWithMessage(CorruptObjectException.class, exceptionMessage, () -> {
            validateVersion(name);
        });
    }

    private String replaceSlashes(String value) {
        var replacement = File.separator;
        if (replacement.equals("\\")) {
            replacement = "\\\\";
        }
        return value.replaceAll("/", replacement);
    }

}
