package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.VersionId;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.InventoryMapper;
import edu.wisc.library.ocfl.core.util.NamasteFileWriter;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashSet;
import java.util.stream.Collectors;

public class FileSystemOcflStorage implements OcflStorage {

    private static final Logger LOG = LoggerFactory.getLogger(FileSystemOcflStorage.class);

    private Path repositoryRoot;
    private ObjectIdPathMapper objectIdPathMapper;
    private InventoryMapper inventoryMapper;
    private NamasteFileWriter namasteFileWriter;

    public FileSystemOcflStorage(Path repositoryRoot, ObjectIdPathMapper objectIdPathMapper) {
        this.repositoryRoot = Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        this.objectIdPathMapper = Enforce.notNull(objectIdPathMapper, "objectIdPathMapper cannot be null");
        this.inventoryMapper = new InventoryMapper();
        this.namasteFileWriter = new NamasteFileWriter();
    }

    public FileSystemOcflStorage setInventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    public FileSystemOcflStorage setNamasteFileWriter(NamasteFileWriter namasteFileWriter) {
        this.namasteFileWriter = Enforce.notNull(namasteFileWriter, "namasteFileWriter cannot be null");
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Inventory loadInventory(String objectId) {
        var objectRootPath = objectRootPath(objectId);

        if (Files.exists(objectRootPath)) {
            return parseInventory(inventoryPath(objectRootPath));
        }

        return null;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void storeNewVersion(Inventory inventory, Path stagingDir) {
        var objectRootPath = objectRootPath(inventory.getId());

        try {
            if (isFirstVersion(inventory)) {
                setupNewObjectDirs(objectRootPath);
            }

            var versionPath = objectRootPath.resolve(inventory.getHead().toString());

            try {
                Files.createDirectory(versionPath);
            } catch (FileAlreadyExistsException e) {
                throw new ObjectOutOfSyncException(
                        String.format("Failed to create a new version of object %s. Changes are out of sync with the current object state.", inventory.getId()));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

            FileUtil.moveDirectory(stagingDir, versionPath);
            headFixityCheck(inventory, objectRootPath);
            copyInventoryToRoot(objectRootPath, inventory);
        } catch (RuntimeException e) {
            rollbackChanges(objectRootPath, inventory);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir) {
        var objectRootPath = objectRootPath(inventory.getId());
        var version = inventory.getVersion(versionId);

        version.getState().forEach((id, files) -> {
            if (!inventory.manifestContainsId(id)) {
                throw new IllegalStateException(String.format("Missing manifest entry for %s in object %s.",
                        id, inventory.getId()));
            }

            var src = inventory.getFilePath(id);
            var srcPath = objectRootPath.resolve(src);

            files.forEach(dstPath -> {
                var path = stagingDir.resolve(dstPath);
                FileUtil.copyFileMakeParents(srcPath, path);

                var digest = computeDigest(path, inventory.getDigestAlgorithm());

                var paths = inventory.getFilePaths(digest);
                if (paths == null || !paths.contains(src)) {
                    throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Was: %s",
                            path, inventory.getId(), inventory.getDigestAlgorithm().getValue(), digest));
                }
            });
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void retrieveFile(Inventory inventory, String fileId, Path destinationPath, OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));
        var objectRootPath = objectRootPath(inventory.getId());

        var filePath = inventory.getFilePath(fileId);

        if (filePath == null) {
            throw new IllegalArgumentException(String.format("File %s does not exist in object %s.", fileId, inventory.getId()));
        }

        try {
            if (options.contains(OcflOption.OVERWRITE)) {
                FileUtil.copyFileMakeParents(objectRootPath.resolve(filePath), destinationPath, StandardCopyOption.REPLACE_EXISTING);
            } else {
                FileUtil.copyFileMakeParents(objectRootPath.resolve(filePath), destinationPath);
            }
            var digest = computeDigest(destinationPath, inventory.getDigestAlgorithm());
            compareDigests(inventory, filePath, fileId, digest);
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(destinationPath);
            throw e;
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void initializeStorage(String ocflVersion) {
        if (!Files.exists(repositoryRoot)) {
            FileUtil.createDirectories(repositoryRoot);
        } else {
            Enforce.expressionTrue(Files.isDirectory(repositoryRoot), repositoryRoot,
                    "repositoryRoot must be a directory");
        }

        if (repositoryRoot.toFile().list().length == 0) {
            // setup new repo
            namasteFileWriter.writeFile(repositoryRoot, ocflVersion);
            // TODO ocfl_1.0.txt
            // TODO ocfl_layout.json
        } else {
            // TODO validate existing repo
        }

        if (!Files.exists(repositoryRoot.resolve(OcflConstants.DEPOSIT_DIRECTORY))) {
            FileUtil.createDirectories(repositoryRoot.resolve(OcflConstants.DEPOSIT_DIRECTORY));
        }

        // TODO add copy of OCFL spec -- ocfl_1.0.txt
        // TODO add storage layout description -- ocfl_layout.json

        // TODO verify can read OCFL version
        // TODO how to verify that the repo is configured correctly to read the layout of an existing structure?
    }

    private Path objectRootPath(String objectId) {
        return repositoryRoot.resolve(objectIdPathMapper.map(objectId));
    }

    private Path inventoryPath(Path rootPath) {
        return rootPath.resolve(OcflConstants.INVENTORY_FILE);
    }

    private Path inventorySidecarPath(Path rootPath, DigestAlgorithm digestAlgorithm) {
        return rootPath.resolve(OcflConstants.INVENTORY_FILE + "." + digestAlgorithm.getValue());
    }

    private Inventory parseInventory(Path inventoryPath) {
        verifyInventory(inventoryPath);
        return inventoryMapper.readValue(inventoryPath);
    }

    private void verifyInventory(Path inventoryPath) {
        var sidecarPath = findInventorySidecar(inventoryPath.getParent());
        var expectedDigest = readInventoryDigest(sidecarPath);
        var algorithm = getDigestAlgorithmFromSidecar(sidecarPath);

        var actualDigest = computeDigest(inventoryPath, algorithm);

        if (!expectedDigest.equalsIgnoreCase(actualDigest)) {
            throw new FixityCheckException(String.format("Invalid inventory file. Expected %s digest: %s; Actual: %s",
                    algorithm.getValue(), expectedDigest, actualDigest));
        }
    }

    private Path findInventorySidecar(Path objectRootPath) {
        try (var files = Files.list(objectRootPath)) {
            var sidecars = files
                    .filter(file -> file.getFileName().toString().startsWith(OcflConstants.INVENTORY_FILE + "."))
                    .collect(Collectors.toList());

            if (sidecars.size() != 1) {
                throw new IllegalStateException(String.format("Expected there to be one inventory sidecar file, but found %s.",
                        sidecars.size()));
            }

            return sidecars.get(0);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String readInventoryDigest(Path inventorySidecarPath) {
        try {
            var parts = Files.readString(inventorySidecarPath).split("\\s");
            if (parts.length == 0) {
                throw new IllegalStateException("Invalid inventory sidecar file: " + inventorySidecarPath);
            }
            return parts[0];
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private DigestAlgorithm getDigestAlgorithmFromSidecar(Path inventorySidecarPath) {
        return DigestAlgorithm.fromValue(
                inventorySidecarPath.getFileName().toString().substring(OcflConstants.INVENTORY_FILE.length() + 1));
    }

    private boolean isFirstVersion(Inventory inventory) {
        return inventory.getVersions().size() == 1;
    }

    private void setupNewObjectDirs(Path objectRootPath) {
        FileUtil.createDirectories(objectRootPath);
        namasteFileWriter.writeFile(objectRootPath, OcflConstants.OCFL_OBJECT_VERSION);
    }

    private void copyInventoryToRoot(Path objectRootPath, Inventory inventory) {
        var versionRoot = objectRootPath.resolve(inventory.getHead().toString());
        var digestAlgorithm = inventory.getDigestAlgorithm();

        copy(inventoryPath(versionRoot), inventoryPath(objectRootPath));
        copy(inventorySidecarPath(versionRoot, digestAlgorithm), inventorySidecarPath(objectRootPath, digestAlgorithm));
    }

    private void headFixityCheck(Inventory inventory, Path objectRootPath) {
        var versionPrefix = inventory.getHead().toString() + "/";
        inventory.getHeadVersion().getState().keySet().forEach(fileId -> {
            inventory.getFilePaths(fileId).forEach(filePath -> {
                if (filePath.startsWith(versionPrefix)) {
                    var digest = computeDigest(objectRootPath.resolve(filePath), inventory.getDigestAlgorithm());
                    if (!fileId.equalsIgnoreCase(digest)) {
                        throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Expected: %s; Actual: %s",
                                filePath, inventory.getId(), inventory.getDigestAlgorithm().getValue(), fileId, digest));
                    }
                }
            });
        });
    }

    private void compareDigests(Inventory inventory, String path, String expected, String actual) {
        if (!expected.equalsIgnoreCase(actual)) {
            throw new FixityCheckException(String.format("File %s in object %s failed its %s fixity check. Expected: %s; Actual: %s",
                    path, inventory.getId(), inventory.getDigestAlgorithm().getValue(), expected, actual));
        }
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void rollbackChanges(Path objectRootPath, Inventory inventory) {
        try {
            FileUtil.safeDeletePath(objectRootPath.resolve(inventory.getHead().toString()));
            if (isFirstVersion(inventory)) {
                FileUtil.safeDeletePath(objectRootPath);
            } else {
                rollbackInventory(objectRootPath, inventory);
            }
        } catch (RuntimeException e) {
            LOG.error("Failed to rollback changes to object {} cleanly.", inventory.getId(), e);
        }
    }

    private void rollbackInventory(Path objectRootPath, Inventory inventory) {
        var versionRoot = objectRootPath.resolve(inventory.getHead().previousVersionId().toString());
        var digestAlgorithm = inventory.getDigestAlgorithm();

        copy(inventoryPath(versionRoot), inventoryPath(objectRootPath));
        copy(inventorySidecarPath(versionRoot, digestAlgorithm), inventorySidecarPath(objectRootPath, digestAlgorithm));
    }

    private void copy(Path src, Path dst) {
        try {
            Files.copy(src, dst, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
