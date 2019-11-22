package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.security.DigestInputStream;
import java.util.*;

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
    private ParallelProcess parallelProcess;
    private ParallelProcess copyParallelProcess;

    private Map<String, Path> stagedFileMap;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ParallelProcess parallelProcess;
        private ParallelProcess copyParallelProcess;

        public Builder parallelProcess(ParallelProcess parallelProcess) {
            this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
            return this;
        }

        public Builder copyParallelProcess(ParallelProcess copyParallelProcess) {
            this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");
            return this;
        }

        public DefaultOcflObjectUpdater build(Inventory inventory, InventoryUpdater inventoryUpdater, Path stagingDir) {
            return new DefaultOcflObjectUpdater(inventory, inventoryUpdater, stagingDir, parallelProcess, copyParallelProcess);
        }

    }

    /**
     * @see Builder
     */
    public DefaultOcflObjectUpdater(Inventory inventory, InventoryUpdater inventoryUpdater, Path stagingDir,
                                    ParallelProcess parallelProcess, ParallelProcess copyParallelProcess) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
        this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
        this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");

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

        var options = new HashSet<>(Arrays.asList(ocflOptions));

        var destination = destinationPath(destinationPath, sourcePath);
        var files = FileUtil.findFiles(sourcePath);

        if (files.size() == 0) {
            throw new IllegalArgumentException(String.format("No files were found under %s to add", sourcePath));
        }

        // TODO extract
        // TODO refactor
        // TODO don't fork if there's only one

        var filesWithDigests = parallelProcess.collection(files, file -> {
            var digest = DigestUtil.computeDigest(inventory.getDigestAlgorithm(), file);
            return Map.entry(file, digest);
        });

        var copyFiles = new HashMap<Path, InventoryUpdater.AddFileResult>();
        var newStagedFiles = new HashMap<String, Path>();

        filesWithDigests.forEach(entry -> {
            var file = entry.getKey();
            var digest = entry.getValue();
            var logicalPath = logicalPath(sourcePath, file, destination);
            var result = inventoryUpdater.addFile(digest, logicalPath, ocflOptions);
            if (result.isNew()) {
                copyFiles.put(file, result);
                newStagedFiles.put(logicalPath, stagingFullPath(result.getPathUnderContentDir()));
            }
        });

        copyParallelProcess.map(copyFiles, (file, result) -> {
            var stagingFullPath = stagingFullPath(result.getPathUnderContentDir());

            if (options.contains(OcflOption.MOVE_SOURCE)) {
                LOG.debug("Moving file <{}> to <{}>", file, stagingFullPath);
                FileUtil.moveFileMakeParents(file, stagingFullPath, StandardCopyOption.REPLACE_EXISTING);
            } else {
                LOG.debug("Copying file <{}> to <{}>", file, stagingFullPath);
                FileUtil.copyFileMakeParents(file, stagingFullPath, StandardCopyOption.REPLACE_EXISTING);
            }
        });

        if (options.contains(OcflOption.MOVE_SOURCE)) {
            // Cleanup empty dirs
            FileUtil.safeDeletePath(sourcePath);
        }

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
        copyInputStream(digestInput, tempPath);

        if (input instanceof FixityCheckInputStream) {
            ((FixityCheckInputStream) input).checkFixity();
        }

        var digest = Hex.encodeHexString(digestInput.getMessageDigest().digest());
        var result = inventoryUpdater.addFile(digest, destinationPath, ocflOptions);

        if (!result.isNew()) {
            LOG.debug("Deleting file <{}> because a file with same digest <{}> is already present in the object", tempPath, digest);
            FileUtil.delete(tempPath);
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
            digest = DigestUtil.computeDigest(algorithm, file);
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
                FileUtil.delete(stagingPath);
            }
        });
    }

    private String logicalPath(Path sourcePath, Path sourceFile, String destinationPath) {
        var sourceRelative = FileUtil.pathToStringStandardSeparator(sourcePath.relativize(sourceFile));
        return FileUtil.pathJoinIgnoreEmpty(destinationPath, sourceRelative);
    }

    private Path stagingFullPath(String pathUnderContentDir) {
        return Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), pathUnderContentDir));
    }

    private String destinationPath(String path, Path sourcePath) {
        if (path.isBlank() && Files.isRegularFile(sourcePath)) {
            return sourcePath.getFileName().toString();
        }
        return path;
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

    private void copyInputStream(InputStream input, Path dst) {
        try {
            Files.copy(input, dst);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
