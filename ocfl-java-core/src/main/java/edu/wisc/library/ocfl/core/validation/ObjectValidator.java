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

import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.InvalidInventoryException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.ObjectPaths;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.inventory.SidecarMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteTypeFile;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

public class ObjectValidator {

    private final InventoryMapper inventoryMapper;

    public ObjectValidator(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
    }

    /**
     * Validates the structure, files, and inventories of an object and all of its versions. This is VERY SLOW.
     *
     * @param objectRoot path to the object root
     * @param inventory deserialized root inventory
     */
    public void validateObject(Path objectRoot, Inventory inventory) {
        InventoryValidator.validateDeep(inventory);
        validateObjectStructure(objectRoot, inventory);
        validateRootInventorySameAsHeadInventory(objectRoot, inventory);
        validateManifest(objectRoot, inventory);
        validateVersions(objectRoot, inventory);
    }

    /**
     * Validates the contents of a version directory.
     *
     * @param versionRoot path to the version directory
     * @param inventory deserialized version inventory
     */
    public void validateVersion(Path versionRoot, Inventory inventory) {
        validateInventory(versionRoot);
        validateVersionRoot(versionRoot);
        validateVersionFiles(versionRoot, inventory);
    }

    private void validateObjectStructure(Path objectRoot, Inventory inventory) {
        var ocflVersion = inventory.getType().getOcflVersion();
        var expectedFiles = new HashSet<Path>();
        expectedFiles.add(validateNamasteFile(ocflVersion, objectRoot));
        validateInventory(objectRoot);

        expectedFiles.add(ObjectPaths.inventoryPath(objectRoot));
        expectedFiles.add(ObjectPaths.inventorySidecarPath(objectRoot, inventory));
        expectedFiles.add(ObjectPaths.logsPath(objectRoot));
        expectedFiles.add(ObjectPaths.extensionsPath(objectRoot));

        inventory.getVersions().keySet().forEach(version -> {
            var versionDir = validatePathExists(objectRoot.resolve(version.toString()));
            validateInventory(versionDir);
            validateVersionRoot(versionDir);
            expectedFiles.add(versionDir);
        });

        try (var walk = Files.list(objectRoot)) {
            walk.filter(file -> !expectedFiles.contains(file)).forEach(file -> {
                throw new CorruptObjectException(String.format("Object %s contains an invalid file: %s",
                        inventory.getId(), file));
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void validateManifest(Path objectRoot, Inventory inventory) {
        var algorithm = inventory.getDigestAlgorithm();

        var excludeDirs = new HashSet<Path>();
        excludeDirs.add(ObjectPaths.extensionsPath(objectRoot));
        excludeDirs.add(ObjectPaths.logsPath(objectRoot));

        var manifestCopy = new HashMap<>(inventory.getManifest());

        try {
            Files.walkFileTree(objectRoot, new SimpleFileVisitor<>() {
                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) {
                    if (excludeDirs.contains(dir)) {
                       return FileVisitResult.SKIP_SUBTREE;
                    } else if (dir.getParent().equals(objectRoot)) {
                        var version = VersionId.fromString(dir.getFileName().toString());
                        if (version.compareTo(inventory.getHead()) > 0) {
                            return FileVisitResult.SKIP_SUBTREE;
                        }
                    }

                    return FileVisitResult.CONTINUE;
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (!(file.getParent().equals(objectRoot) || file.getParent().getParent().equals(objectRoot))) {
                        var digest = DigestUtil.computeDigestHex(algorithm, file);
                        var contentPaths = inventory.getContentPaths(digest);
                        if (contentPaths.isEmpty()) {
                            throw new CorruptObjectException(String.format("Object %s contains an unexpected file: %s",
                                    inventory.getId(), file));
                        }
                        for (var contentPath : contentPaths) {
                            if (file.equals(objectRoot.resolve(contentPath))) {
                                if (manifestCopy.get(digest).size() == 1) {
                                    manifestCopy.remove(digest);
                                } else {
                                    manifestCopy.get(digest).remove(contentPath);
                                }
                                return FileVisitResult.CONTINUE;
                            }
                        }

                        throw new CorruptObjectException(String.format("File %s has unexpected %s digest value %s.",
                                file, algorithm.getOcflName(), digest));
                    }

                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        if (!manifestCopy.isEmpty()) {
            throw new CorruptObjectException(String.format("The following files are defined in object %s's manifest, but are not found on disk: %s",
                    inventory.getId(), manifestCopy));
        }
    }

    private void validateVersionFiles(Path versionRoot, Inventory inventory) {
        var contentDirectory = versionRoot.resolve(inventory.resolveContentDirectory());
        var contentPathPrefix = FileUtil.pathJoinFailEmpty(inventory.getHead().toString(), inventory.resolveContentDirectory());
        var expectedFileIds = inventory.getFileIdsForMatchingFiles(contentPathPrefix);
        var expectedManifest = new HashMap<String, Set<String>>(expectedFileIds.size());

        for (var fileId : expectedFileIds) {
            var filteredContentPaths = inventory.getContentPaths(fileId).stream()
                    .filter(path -> path.startsWith(contentPathPrefix))
                    .collect(Collectors.toSet());
            expectedManifest.put(fileId, filteredContentPaths);
        }

        if (Files.exists(contentDirectory)) {
            try (var files = Files.walk(contentDirectory)) {
                files.filter(Files::isRegularFile).forEach(file -> {
                    var contentRelative = contentDirectory.relativize(file);
                    var contentPath = FileUtil.pathJoinFailEmpty(contentPathPrefix, FileUtil.pathToStringStandardSeparator(contentRelative));
                    var expectedDigest = inventory.getFileId(contentPath);

                    if (expectedDigest == null) {
                        throw new CorruptObjectException(String.format("Object %s contains an unexpected file: %s",
                                inventory.getId(), file));
                    }

                    var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), file);

                    if (!expectedDigest.equalsIgnoreCase(digest)) {
                        throw new CorruptObjectException(String.format("File %s has unexpected %s digest value. Expected: %s; Actual: %s.",
                                file, inventory.getDigestAlgorithm().getOcflName(), expectedDigest, digest));
                    }

                    if (expectedManifest.get(digest).size() == 1) {
                        expectedManifest.remove(digest);
                    } else {
                        expectedManifest.get(digest).remove(contentPath);
                    }
                });
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        if (!expectedManifest.isEmpty()) {
            throw new CorruptObjectException(String.format("The following files are defined in object %s's manifest, but are not found on disk: %s",
                    inventory.getId(), expectedManifest));
        }
    }

    private void validateVersions(Path objectRoot, Inventory inventory) {
        var currentInventory = inventory;

        while (!VersionId.V1.equals(currentInventory.getHead())) {
            var previous = currentInventory.getHead().previousVersionId();
            var inventoryPath = ObjectPaths.inventoryPath(objectRoot.resolve(previous.toString()));
            // Don't care about a valid digest here
            var previousInventory = inventoryMapper.read(inventory.getObjectRootPath(), "digest", inventoryPath);

            InventoryValidator.validateDeep(previousInventory);
            validateId(inventory, previousInventory);
            validateVersionNumber(previous, previousInventory, inventoryPath);

            if (!Objects.equals(currentInventory.getDigestAlgorithm(), previousInventory.getDigestAlgorithm())) {
                validateManifest(objectRoot, previousInventory);
                validateVersionStatesByContentPath(currentInventory, previousInventory);
            } else {
                InventoryValidator.validateVersionStates(currentInventory, previousInventory);
            }

            currentInventory = previousInventory;
        }
    }

    private Path validateNamasteFile(OcflVersion ocflVersion, Path objectRoot) {
        var namasteFile = new NamasteTypeFile(ocflVersion.getOcflObjectVersion());
        var namasteFilePath = validatePathExists(objectRoot.resolve(namasteFile.fileName()));
        var actualContent = content(namasteFilePath);
        if (Objects.equals(namasteFile.fileContent(), actualContent.trim())) {
            throw new CorruptObjectException("Invalid namaste file at " + namasteFilePath);
        }
        return namasteFilePath;
    }

    private void validateInventory(Path path) {
        var inventory = validatePathExists(ObjectPaths.inventoryPath(path));
        var sidecar = validatePathExists(ObjectPaths.findInventorySidecarPath(path));
        var algorithm = SidecarMapper.getDigestAlgorithmFromSidecar(FileUtil.pathToStringStandardSeparator(sidecar));
        validateInventoryDigest(inventory, sidecar, algorithm);
    }

    private void validateInventoryDigest(Path inventory, Path sidecar, DigestAlgorithm algorithm) {
        var expected = SidecarMapper.readDigest(sidecar);
        var actual = DigestUtil.computeDigestHex(algorithm, inventory);
        if (!expected.equalsIgnoreCase(actual)) {
            throw new CorruptObjectException(String.format("Inventory file at %s does not match expected %s digest: Expected %s; Actual %s",
                    inventory, algorithm.getOcflName(), expected, actual));
        }
    }

    private void validateVersionRoot(Path versionRoot) {
        var inventory = ObjectPaths.inventoryPath(versionRoot).getFileName().toString();
        var sidecar = ObjectPaths.findInventorySidecarPath(versionRoot).getFileName().toString();

        try (var files = Files.list(versionRoot)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                var name = file.getFileName().toString();
                if (!(name.equals(inventory) || name.equals(sidecar))) {
                    throw new CorruptObjectException("Version contains an illegal file at " + file);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void validateVersionStatesByContentPath(Inventory currentInventory, Inventory previousInventory) {
        var current = previousInventory.getHead();
        while (true) {
            var currentCopy = current;
            var currentVersion = currentInventory.getVersion(current);
            var currentState = currentVersion.getState();
            var previousState = previousInventory.getVersion(current).getState();

            previousState.values().stream().flatMap(Collection::stream).forEach(previousLogical -> {
                if (currentVersion.getFileId(previousLogical) == null) {
                    throw versionMismatchException(currentInventory, previousInventory, currentCopy);
                }
            });

            currentState.forEach((currentDigest, currentLogical) -> {
                var currentContentPaths = currentInventory.getContentPaths(currentDigest);
                for (var currentContentPath : currentContentPaths) {
                    var previousDigest = previousInventory.getFileId(currentContentPath);
                    var previousLogical = previousState.get(previousDigest);
                    if (!Objects.equals(currentLogical, previousLogical)) {
                        throw versionMismatchException(currentInventory, previousInventory, currentCopy);
                    }
                }
            });

            if (VersionId.V1.equals(current)) {
                break;
            }

            current = current.previousVersionId();
        }
    }

    private Path validatePathExists(Path path) {
        if (!Files.exists(path)) {
            throw new CorruptObjectException(String.format("Expected file %s to exist, but it does not.", path));
        }
        return path;
    }

    private void validateRootInventorySameAsHeadInventory(Path objectRoot, Inventory inventory) {
        var rootDigest = content(ObjectPaths.findInventorySidecarPath(objectRoot));
        var headDigest = content(ObjectPaths.findInventorySidecarPath(objectRoot.resolve(inventory.getHead().toString())));
        if (!rootDigest.equalsIgnoreCase(headDigest)) {
            throw new CorruptObjectException(
                    String.format("The inventory file in the object root of object %s does not match the inventory in its HEAD version, %s",
                            inventory.getId(), inventory.getHead()));
        }
    }

    private void validateId(Inventory currentInventory, Inventory previousInventory) {
        if (!Objects.equals(currentInventory.getId(), previousInventory.getId())) {
            throw new CorruptObjectException(String.format("Versions %s and %s of object %s have different object IDs, %s and %s.",
                    currentInventory.getHead(), previousInventory.getHead(),
                    currentInventory.getId(), currentInventory.getId(), previousInventory.getId()));
        }
    }

    private void validateVersionNumber(VersionId expectedId, Inventory inventory, Path inventoryPath) {
        if (!Objects.equals(expectedId.toString(), inventory.getHead().toString())) {
            throw new CorruptObjectException(String.format("Expected version %s but was %s in %s.",
                    expectedId, inventory.getHead(), inventoryPath));
        }
    }

    private String content(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private InvalidInventoryException versionMismatchException(Inventory currentInventory, Inventory previousInventory, VersionId current) {
        return new InvalidInventoryException(String.format("In object %s the inventories in version %s and %s define a different state for version %s.",
                currentInventory.getId(), currentInventory.getHead(), previousInventory.getHead(), current));
    }

}
