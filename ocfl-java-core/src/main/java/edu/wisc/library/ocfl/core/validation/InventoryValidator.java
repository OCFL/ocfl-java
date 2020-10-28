/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.validation;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionNum;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.User;
import edu.wisc.library.ocfl.core.model.Version;

import java.util.Collection;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Validates that an {@link Inventory} meets the minimum requirements to be serialized as valid OCFL inventory.
 */
public final class InventoryValidator {

    private static final VersionNum VERSION_ZERO = VersionNum.fromString("v0");
    private static final Pattern CONTENT_DIR_PATTERN = Pattern.compile(".*[/\\\\].*");

    private InventoryValidator() {

    }

    /**
     * Validates that an {@link Inventory} meets the minimum requirements to be serialized as valid OCFL inventory.
     * Only the HEAD version is validated.
     *
     * @param inventory the inventory to validate
     * @return the same inventory
     * @throws InvalidInventoryException when validation fails
     */
    public static Inventory validateShallow(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");

        notBlank(inventory.getId(), "Object ID cannot be blank");
        notNull(inventory.getType(), "Type cannot be null");
        notNull(inventory.getHead(), "HEAD cannot be null");
        isTrue(!inventory.getHead().equals(VERSION_ZERO), "HEAD version must be greater than v0");
        validateDigestAlgorithm(inventory.getDigestAlgorithm());
        validateContentDirectory(inventory.getContentDirectory());

        notNull(inventory.getManifest(), "Manifest cannot be null");
        validateVersions(inventory, false);

        return inventory;
    }

    /**
     * Validates the entire {@link Inventory}. This is VERY SLOW.
     *
     * @param inventory the inventory to validate
     * @return the same inventory
     * @throws InvalidInventoryException when validation fails
     */
    public static Inventory validateDeep(Inventory inventory) {
        Enforce.notNull(inventory, "inventory cannot be null");

        notBlank(inventory.getId(), "Object ID cannot be blank");
        notNull(inventory.getType(), "Type cannot be null");
        notNull(inventory.getHead(), "HEAD cannot be null");
        isTrue(!inventory.getHead().equals(VERSION_ZERO), "HEAD version must be greater than v0");
        validateDigestAlgorithm(inventory.getDigestAlgorithm());
        validateContentDirectory(inventory.getContentDirectory());
        validateVersionNumbers(inventory);

        validateFixity(inventory);
        notNull(inventory.getManifest(), "Manifest cannot be null");
        validateVersions(inventory, true);
        validateManifest(inventory);

        return inventory;
    }

    /**
     * Validates that the two inventories contain the same version states, excluding the version in the current
     * that is not in the previous.
     *
     * @param currentInventory current inventory
     * @param previousInventory inventory immediately prior to the current inventory
     */
    public static void validateVersionStates(Inventory currentInventory, Inventory previousInventory) {
        var current = previousInventory.getHead();
        var currentVersions = currentInventory.getVersions().keySet().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));
        var previousVersions = previousInventory.getVersions().keySet().stream()
                .collect(Collectors.toMap(Function.identity(), Function.identity()));

        while (true) {
            validateVersionNumber(currentInventory.getId(), currentVersions.get(current), previousVersions.get(current));
            var currentState = currentInventory.getVersion(current).getState();
            var previousState = previousInventory.getVersion(current).getState();
            if (!Objects.equals(currentState, previousState)) {
                throw new InvalidInventoryException(String.format("In object %s the inventories in version %s and %s define a different state for version %s.",
                        currentInventory.getId(), currentInventory.getHead(), previousInventory.getHead(), current));
            }
            if (VersionNum.V1.equals(current)) {
                break;
            }
            current = current.previousVersionNum();
        }
    }

    /**
     * Validates that the currentInventory is a valid next inventory state from the previousInventory. The must have
     * the same id, type, digestAlgorithm, and contentDirectory values. Additionally, the currentInventory must be
     * one version later than the previous, and all of the versions they have in common must be identical.
     *
     * @param currentInventory current inventory
     * @param previousInventory inventory immediately prior to the current inventory
     */
    public static void validateCompatibleInventories(Inventory currentInventory, Inventory previousInventory) {
        areEqual(currentInventory.getId(), previousInventory.getId(),
                String.format("Object IDs are not the same. Existing: %s; New: %s",
                        previousInventory.getId(), currentInventory.getId()));
        areEqual(currentInventory.getType(), previousInventory.getType(),
                String.format("Inventory types are not the same. Existing: %s; New: %s",
                        previousInventory.getType().getId(), currentInventory.getType().getId()));
        areEqual(currentInventory.getDigestAlgorithm(), previousInventory.getDigestAlgorithm(),
                String.format("Inventory digest algorithms are not the same. Existing: %s; New: %s",
                        previousInventory.getDigestAlgorithm().getOcflName(), currentInventory.getDigestAlgorithm().getOcflName()));
        areEqual(currentInventory.getContentDirectory(), previousInventory.getContentDirectory(),
                String.format("Inventory content directories are not the same. Existing: %s; New: %s",
                        previousInventory.getContentDirectory(), currentInventory.getContentDirectory()));
        if (!Objects.equals(currentInventory.getHead(), previousInventory.nextVersionNum())) {
            throw new InvalidInventoryException(String.format(
                    "The new HEAD inventory version must be the next sequential version number. Existing: %s; New: %s",
                    previousInventory.getHead(), currentInventory.getHead()));
        }
        validateVersionStates(currentInventory, previousInventory);
    }

    private static void validateFixity(Inventory inventory) {
        var fixityMap = inventory.getFixity();

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

    private static void validateVersions(Inventory inventory, boolean allVersions) {
        var versionMap = inventory.getVersions();
        notEmpty(versionMap, "Versions cannot be empty");

        for (var i = 1; i <= versionMap.size(); i++) {
            var versionNum = new VersionNum(i);
            notNull(versionMap.get(versionNum), String.format("Version %s is missing", versionNum));
        }

        var expectedHead = new VersionNum(versionMap.size());
        isTrue(inventory.getHead().equals(expectedHead), String.format("HEAD must be the latest version. Expected: %s; Was: %s",
                expectedHead, inventory.getHead()));

        if (!allVersions) {
            validateVersion(inventory, versionMap.get(expectedHead), expectedHead);
        } else {
            versionMap.forEach((versionNum, version) -> {
                validateVersion(inventory, version, versionNum);
            });
        }
    }

    private static void validateVersion(Inventory inventory, Version version, VersionNum versionNum) {
        notNull(version, String.format("Version %s is missing", versionNum));
        notNull(version.getCreated(), String.format("Version created timestamp in version %s cannot be null", versionNum));
        validateUser(version.getUser(), versionNum);

        var state = version.getState();
        notNull(state, String.format("Version state in version %s cannot be null", versionNum));

        state.forEach((digest, logicalPaths) -> {
            notEmpty(logicalPaths, String.format("Version state logical paths in version %s cannot be empty", versionNum));
            notNull(inventory.getContentPath(digest),
                    String.format("Version state entry %s => %s in version %s does not have a corresponding entry in the manifest block.",
                            digest, logicalPaths, versionNum));
        });
    }

    private static void validateManifest(Inventory inventory) {
        var stateFileIds = inventory.getVersions().values().stream()
                .flatMap(v -> v.getState().keySet().stream())
                .collect(Collectors.toSet());

        inventory.getManifest().keySet().forEach(fileId -> {
            if (!stateFileIds.contains(fileId)) {
                throw new CorruptObjectException(String.format("Object %s's manifest contains an entry for %s, but it is not referenced in any version's state.",
                        inventory.getId(), fileId));
            }
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

    private static void validateUser(User user, VersionNum versionNum) {
        if (user != null) {
            notBlank(user.getName(), String.format("User name in version %s cannot be blank", versionNum));
        }
    }

    private static void validateVersionNumbers(Inventory inventory) {
        var expectedPadding = inventory.getHead().getZeroPaddingWidth();

        inventory.getVersions().keySet().forEach(versionNumber -> {
            if (versionNumber.getZeroPaddingWidth() != expectedPadding) {
                throw new InvalidInventoryException(String.format("%s is not zero-padded correctly. Expected: %s; Actual: %s",
                        versionNumber, expectedPadding, versionNumber.getZeroPaddingWidth()));
            }
        });
    }

    private static void validateVersionNumber(String objectId, VersionNum currentVersion, VersionNum previousVersion) {
        if (!Objects.equals(currentVersion.toString(), previousVersion.toString())) {
            throw new InvalidInventoryException(String.format("Object %s's version number formatting differs: %s vs %s",
                    objectId, previousVersion, currentVersion));
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

    private static <T> void areEqual(T left, T right, String message) {
        if (!Objects.equals(left, right)) {
            throw new InvalidInventoryException(message);
        }
    }

}
