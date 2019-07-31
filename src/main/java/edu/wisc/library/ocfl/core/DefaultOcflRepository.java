package edu.wisc.library.ocfl.core;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.CommitMessage;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryType;
import edu.wisc.library.ocfl.core.model.VersionId;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

public class DefaultOcflRepository implements OcflRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflRepository.class);

    private OcflStorage storage;
    private ObjectMapper objectMapper;
    private Path workDir;

    private Set<DigestAlgorithm> fixityAlgorithms;
    private InventoryType inventoryType;
    private DigestAlgorithm digestAlgorithm;
    private String contentDirectory;

    public DefaultOcflRepository(OcflStorage storage, ObjectMapper objectMapper,
                                 Path workDir, Set<DigestAlgorithm> fixityAlgorithms,
                                 InventoryType inventoryType, DigestAlgorithm digestAlgorithm,
                                 String contentDirectory) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        this.fixityAlgorithms = Enforce.notNull(fixityAlgorithms, "fixityAlgorithms cannot be null");
        this.inventoryType = Enforce.notNull(inventoryType, "inventoryType cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
    }

    public void putObject(String objectId, Path path, CommitMessage commitMessage) {
        // TODO additional id restrictions? eg must contain at least 1 alpha numeric character, max length?
        Enforce.notBlank(objectId, "objectId cannot be blank");
        Enforce.notNull(path, "path cannot be null");

        // TODO handle race conditions
        // TODO what about across processes?

        var inventory = storage.loadInventory(objectId);

        if (inventory == null) {
            inventory = new Inventory()
                    .setId(objectId)
                    .setType(inventoryType)
                    .setDigestAlgorithm(digestAlgorithm)
                    .setContentDirectory(contentDirectory);
        }

        var stagingDir = stageNewVersion(inventory, path, commitMessage);

        try {
            storage.storeNewVersion(inventory, stagingDir);
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(stagingDir);
            throw e;
        }
    }

    @Override
    public void getObject(String objectId, Path outputPath) {
        Enforce.notBlank(objectId, "objectId cannot be blank");
        Enforce.notNull(outputPath, "outputPath cannot be null");
        Enforce.expressionTrue(Files.exists(outputPath), outputPath, "outputPath must exist");
        Enforce.expressionTrue(Files.isDirectory(outputPath), outputPath, "outputPath must be a directory");

        var inventory = requireInventory(objectId);
        getObjectInternal(inventory, inventory.getHead(), outputPath);
    }

    @Override
    public void getObject(String objectId, String versionIdStr, Path outputPath) {
        Enforce.notBlank(objectId, "objectId cannot be blank");
        Enforce.notBlank(versionIdStr, "versionId cannot be blank");
        Enforce.notNull(outputPath, "outputPath cannot be null");
        Enforce.expressionTrue(Files.exists(outputPath), outputPath, "outputPath must exist");
        Enforce.expressionTrue(Files.isDirectory(outputPath), outputPath, "outputPath must be a directory");

        var versionId = VersionId.fromValue(versionIdStr);

        var inventory = requireInventory(objectId);
        getObjectInternal(inventory, versionId, outputPath);
    }

    private Inventory requireInventory(String objectId) {
        var inventory = storage.loadInventory(objectId);
        if (inventory == null) {
            // TODO modeled exception
            throw new IllegalArgumentException(String.format("Object %s was not found.", objectId));
        }
        return inventory;
    }

    private Path stageNewVersion(Inventory inventory, Path path, CommitMessage commitMessage) {
        var stagingDir = FileUtil.createTempDir(workDir, inventory.getId());

        var inventoryUpdater = InventoryUpdater.newVersion(inventory);
        inventoryUpdater.addCommitMessage(commitMessage);

        var files = identifyFiles(path);
        // TODO handle case when no files. is valid?

        var contentDir = FileUtil.createDirectories(stagingDir.resolve(inventory.getContentDirectory()));

        for (var file : files) {
            var relativePath = path.relativize(file);
            var isNewFile = inventoryUpdater.addFile(file, relativePath, fixityAlgorithms);

            if (isNewFile) {
                FileUtil.copyFileMakeParents(file, contentDir.resolve(relativePath));
            }
        }

        writeInventory(inventory, stagingDir);

        return stagingDir;
    }

    private void getObjectInternal(Inventory inventory, VersionId versionId, Path outputPath) {
        var fileMap = resolveVersionContents(inventory, versionId);
        var stagingDir = FileUtil.createTempDir(workDir, inventory.getId());

        try {
            storage.reconstructObjectVersion(inventory, fileMap, stagingDir);

            FileUtil.moveDirectory(stagingDir, outputPath);
        } catch (RuntimeException e) {
            FileUtil.safeDeletePath(stagingDir);
            throw e;
        }
    }

    private Map<String, Set<String>> resolveVersionContents(Inventory inventory, VersionId versionId) {
        var manifest = inventory.getManifest();
        var version = inventory.getVersions().get(versionId);

        if (version == null) {
            // TODO modeled exception
            throw new IllegalArgumentException(String.format("Object %s version %s was not found.",
                    inventory.getId(), versionId));
        }

        var fileMap = new HashMap<String, Set<String>>();

        version.getState().forEach((id, files) -> {
            if (!manifest.containsKey(id)) {
                throw new IllegalStateException(String.format("Missing manifest entry for %s in object %s.",
                        id, inventory.getId()));
            }

            var source = manifest.get(id).iterator().next();
            fileMap.put(source, files);
        });

        return fileMap;
    }

    private List<Path> identifyFiles(Path path) {
        var files = new ArrayList<Path>();

        if (Files.isDirectory(path)) {
            try (var paths = Files.walk(path)) {
                paths.filter(Files::isRegularFile)
                        .forEach(files::add);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } else {
            files.add(path);
        }

        return files;
    }

    private void writeInventory(Inventory inventory, Path tempDir) {
        try {
            var inventoryPath = tempDir.resolve(OcflConstants.INVENTORY_FILE);
            objectMapper.writeValue(inventoryPath.toFile(), inventory);
            String inventoryDigest = computeDigest(inventoryPath, inventory.getDigestAlgorithm());
            Files.writeString(
                    tempDir.resolve(OcflConstants.INVENTORY_FILE + "." + inventory.getDigestAlgorithm().getValue()),
                    inventoryDigest + "\t" + OcflConstants.INVENTORY_FILE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private String computeDigest(Path path, DigestAlgorithm algorithm) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
