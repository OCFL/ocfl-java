package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.User;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Validates that an {@link Inventory} meets the minimum requirements to be serialized as valid OCFL inventory.
 */
public final class InventoryValidator {

    private static final VersionId VERSION_ZERO = VersionId.fromString("v0");
    private static final Pattern CONTENT_DIR_PATTERN = Pattern.compile(".*[/\\\\].*");

    private InventoryValidator() {

    }

    /**
     * Validates that an {@link Inventory} meets the minimum requirements to be serialized as valid OCFL inventory.
     *
     * @param inventory the inventory to validate
     * @return the same inventory
     * @throws InvalidInventoryException when validation fails
     */
    public static Inventory validate(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");

        notBlank(inventory.getId(), "Object ID cannot be blank");
        notNull(inventory.getType(), "Type cannot be null");
        notNull(inventory.getHead(), "HEAD cannot be null");
        isTrue(!inventory.getHead().equals(VERSION_ZERO), "HEAD version must be greater than v0");
        validateDigestAlgorithm(inventory.getDigestAlgorithm());
        validateContentDirectory(inventory.getContentDirectory());

        validateFixity(inventory);
        notNull(inventory.getManifest(), "Manifest cannot be null");
        validateVersions(inventory);

        return inventory;
    }

    private static void validateFixity(Inventory inventory) {
        var fixityMap = inventory.getFixity();

        // TODO this may be very slow as well...
        if (fixityMap != null) {
            fixityMap.forEach((algorithm, map) -> {
                map.forEach((digest, contentPaths) -> {
                    notEmpty(contentPaths, "Fixity content paths cannot be empty");
                    contentPaths.forEach(contentPath -> {
                        notNull(inventory.getFileId(contentPath),
                                String.format("Fixity entry %s => {%s => %s} does not have a corresponding entry in the manifest block.",
                                        algorithm.getOcflName(), digest, contentPath));
                    });
                });
            });
        }
    }

    private static void validateVersions(Inventory inventory) {
        var versionMap = inventory.getVersions();
        notEmpty(versionMap, "Versions cannot be empty");

        for (var i = 1; i <= versionMap.size(); i++) {
            var versionId = new VersionId(i);
            notNull(versionMap.get(versionId), String.format("Version %s is missing", versionId));
        }

        var expectedHead = new VersionId(versionMap.size());
        isTrue(inventory.getHead().equals(expectedHead), String.format("HEAD must be the latest version. Expected: %s; Was: %s",
                expectedHead, inventory.getHead()));

        // TODO only doing a complete validation on the most recent version because validating all of the versions
        // TODO can be very slow when there are a lot of files and versions
        validateVersion(inventory, versionMap.get(expectedHead), expectedHead);
    }

    private static void validateVersion(Inventory inventory, Version version, VersionId versionId) {
        notNull(version, String.format("Version %s is missing", versionId));
        notNull(version.getCreated(), String.format("Version created timestamp in version %s cannot be null", versionId));
        validateUser(version.getUser(), versionId);

        var state = version.getState();
        notNull(state, String.format("Version state in version %s cannot be null", versionId));

        state.forEach((digest, logicalPaths) -> {
            notEmpty(logicalPaths, String.format("Version state logical paths in version %s cannot be empty", versionId));
            notNull(inventory.getContentPath(digest),
                    String.format("Version state entry %s => %s in version %s does not have a corresponding entry in the manifest block.",
                            digest, logicalPaths, versionId));
        });
    }

    private static void validateDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        notNull(digestAlgorithm, "Digest algorithm cannot be null");
        isTrue(OcflConstants.ALLOWED_DIGEST_ALGORITHMS.contains(digestAlgorithm),
                String.format("Digest algorithm must be one of: %s; Found: %s",
                        OcflConstants.ALLOWED_DIGEST_ALGORITHMS, digestAlgorithm));
    }

    private static void validateContentDirectory(String contentDirectory) {
        if (contentDirectory != null) {
            notBlank(contentDirectory, "Content directory cannot be blank");
            isTrue(!CONTENT_DIR_PATTERN.matcher(contentDirectory).matches(),
                    "Content directory cannot contain / or \\. Found: " + contentDirectory);
        }
    }

    private static void validateUser(User user, VersionId versionId) {
        if (user != null) {
            notBlank(user.getName(), String.format("User name in version %s cannot be blank", versionId));
        }
    }

    private static void notNull(Object object, String message) {
        if (object == null) {
            throw new InvalidInventoryException(message);
        }
    }

    private static void notBlank(String value, String message) {
        if (value == null || value.isBlank()) {
            throw new InvalidInventoryException(message);
        }
    }

    private static void notEmpty(Collection<?> collection, String message) {
        if (collection == null || collection.isEmpty()) {
            throw new InvalidInventoryException(message);
        }
    }

    private static void notEmpty(Map<?, ?> map, String message) {
        if (map == null || map.isEmpty()) {
            throw new InvalidInventoryException(message);
        }
    }

    private static void isTrue(boolean test, String message) {
        if (!test) {
            throw new InvalidInventoryException(message);
        }
    }

}
