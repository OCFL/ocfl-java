package edu.wisc.library.ocfl.core;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.AddFileProcessor;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.SafeFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

/**
 * Default implementation of OcflObjectUpdater that is used by DefaultOcflRepository to provide write access to an object.
 *
 * <p>This class is NOT thread safe.
 */
public class DefaultOcflObjectUpdater implements OcflObjectUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectUpdater.class);

    private Inventory inventory;
    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;
    private AddFileProcessor addFileProcessor;

    private Map<String, Path> stagedFileMap;

    public DefaultOcflObjectUpdater(Inventory inventory, InventoryUpdater inventoryUpdater, Path stagingDir,
                                    AddFileProcessor addFileProcessor) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
        this.addFileProcessor = Enforce.notNull(addFileProcessor, "addFileProcessor cannot be null");

        this.stagedFileMap = new HashMap<>();
    }

    @Override
    public OcflObjectUpdater addPath(Path sourcePath, OcflOption... ocflOptions) {
        return addPath(sourcePath, "", ocflOptions);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notNull(sourcePath, "sourcePath cannot be null");
        Enforce.notNull(destinationPath, "destinationPath cannot be null");

        var newStagedFiles = addFileProcessor.processPath(sourcePath, destinationPath, ocflOptions);
        stagedFileMap.putAll(newStagedFiles);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater writeFile(InputStream input, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notNull(input, "input cannot be null");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var tempPath = stagingDir.resolve(UUID.randomUUID().toString());
        var digestInput = wrapInDigestInputStream(input);
        LOG.debug("Writing input stream to temp file: {}", tempPath);
        SafeFiles.copy(digestInput, tempPath);

        if (input instanceof FixityCheckInputStream) {
            ((FixityCheckInputStream) input).checkFixity();
        }

        var digest = Bytes.wrap(digestInput.getMessageDigest().digest()).encodeHex();
        var result = inventoryUpdater.addFile(digest, destinationPath, ocflOptions);

        if (!result.isNew()) {
            LOG.debug("Deleting file <{}> because a file with same digest <{}> is already present in the object", tempPath, digest);
            SafeFiles.delete(tempPath);
        } else {
            var stagingFullPath = stagingFullPath(result.getPathUnderContentDir());
            LOG.debug("Moving file <{}> to <{}>", tempPath, stagingFullPath);
            FileUtil.moveFileMakeParents(tempPath, stagingFullPath, StandardCopyOption.REPLACE_EXISTING);
            stagedFileMap.put(destinationPath, stagingFullPath);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        var results = inventoryUpdater.removeFile(path);
        removeUnneededStagedFiles(results);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var results = inventoryUpdater.renameFile(sourcePath, destinationPath, ocflOptions);
        removeUnneededStagedFiles(results);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater reinstateFile(VersionId sourceVersionId, String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notNull(sourceVersionId, "sourceVersionId cannot be null");
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var results = inventoryUpdater.reinstateFile(sourceVersionId, sourcePath, destinationPath, ocflOptions);
        removeUnneededStagedFiles(results);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater clearVersionState() {
        inventoryUpdater.clearState();
        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater addFileFixity(String logicalPath, DigestAlgorithm algorithm, String value) {
        Enforce.notBlank(logicalPath, "logicalPath cannot be blank");
        Enforce.notNull(algorithm, "algorithm cannot be null");
        Enforce.notBlank(value, "value cannot be null");

        var digest = inventoryUpdater.getFixityDigest(logicalPath, algorithm);
        var alreadyExists = true;

        if (digest == null) {
            alreadyExists = false;

            if (!stagedFileMap.containsKey(logicalPath)) {
                throw new IllegalStateException(
                        String.format("%s was not newly added in the current block. Fixity information can only be added on new files.", logicalPath));
            }

            if (!algorithm.hasJavaStandardName()) {
                throw new IllegalArgumentException("The specified digest algorithm is not mapped to a Java name: " + algorithm);
            }

            var file = stagedFileMap.get(logicalPath);

            LOG.debug("Computing {} hash of {}", algorithm.getJavaStandardName(), file);
            digest = DigestUtil.computeDigestHex(algorithm, file);
        }

        if (!value.equalsIgnoreCase(digest)) {
            throw new FixityCheckException(String.format("Expected %s digest of %s to be %s, but was %s.",
                    algorithm.getJavaStandardName(), logicalPath, value, digest));
        }

        if (!alreadyExists) {
            inventoryUpdater.addFixity(logicalPath, algorithm, digest);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater clearFixityBlock() {
        inventoryUpdater.clearFixity();
        return this;
    }

    private void removeUnneededStagedFiles(Set<InventoryUpdater.RemoveFileResult> removeFiles) {
        removeFiles.forEach(remove -> {
            var stagingPath = stagingFullPath(remove.getPathUnderContentDir());
            if (Files.exists(stagingPath)) {
                LOG.debug("Deleting {} because it was added and then removed in the same version.", stagingPath);
                SafeFiles.delete(stagingPath);
            }
        });
    }

    private Path stagingFullPath(String pathUnderContentDir) {
        return Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), pathUnderContentDir));
    }

    private DigestInputStream wrapInDigestInputStream(InputStream input) {
        if (input instanceof DigestInputStream) {
            var digestAlgorithm = ((DigestInputStream) input).getMessageDigest().getAlgorithm();
            if (inventory.getDigestAlgorithm().getJavaStandardName().equalsIgnoreCase(digestAlgorithm)) {
                return (DigestInputStream) input;
            }
        }

        return new DigestInputStream(input, inventory.getDigestAlgorithm().getMessageDigest());
    }

}
