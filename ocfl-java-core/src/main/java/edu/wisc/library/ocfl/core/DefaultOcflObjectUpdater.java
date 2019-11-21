package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
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

    private Set<DigestAlgorithm> fixityAlgorithms;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private ParallelProcess parallelProcess;
        private ParallelProcess copyParallelProcess;
        private Set<DigestAlgorithm> fixityAlgorithms;

        public Builder parallelProcess(ParallelProcess parallelProcess) {
            this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
            return this;
        }

        public Builder copyParallelProcess(ParallelProcess copyParallelProcess) {
            this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");
            return this;
        }

        public Builder fixityAlgorithms(Set<DigestAlgorithm> fixityAlgorithms) {
            this.fixityAlgorithms = fixityAlgorithms;
            return this;
        }

        public DefaultOcflObjectUpdater build(Inventory inventory, InventoryUpdater inventoryUpdater, Path stagingDir) {
            return new DefaultOcflObjectUpdater(inventory, inventoryUpdater, stagingDir, parallelProcess, copyParallelProcess, fixityAlgorithms);
        }

    }

    /**
     * @see Builder
     */
    public DefaultOcflObjectUpdater(Inventory inventory, InventoryUpdater inventoryUpdater, Path stagingDir,
                                    ParallelProcess parallelProcess, ParallelProcess copyParallelProcess,
                                    Set<DigestAlgorithm> fixityAlgorithms) {
        this.inventory = Enforce.notNull(inventory, "inventory cannot be null");
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
        this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
        this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");
        this.fixityAlgorithms = fixityAlgorithms == null ? Collections.emptySet() : fixityAlgorithms;
    }

    // TODO add method for adding fixity

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notNull(sourcePath, "sourcePath cannot be null");
        Enforce.notNull(destinationPath, "destinationPath cannot be null");

        var options = new HashSet<>(Arrays.asList(ocflOptions));

        if (destinationPath.isBlank() && Files.isRegularFile(sourcePath)) {
            throw new IllegalArgumentException("A non-blank destination path must be specified when adding a file.");
        }

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

        filesWithDigests.forEach(entry -> {
            var file = entry.getKey();
            var digest = entry.getValue();
            var logicalPath = logicalPath(sourcePath, file, destinationPath);
            var result = inventoryUpdater.addFile(digest, logicalPath, ocflOptions);
            if (result.isNew()) {
                copyFiles.put(file, result);
            }
        });

        parallelProcess.map(copyFiles, (file, result) -> {
            for (var fixityAlgorithm : fixityAlgorithms) {
                if (!inventory.getDigestAlgorithm().equals(fixityAlgorithm)) {
                    if (fixityAlgorithm.hasJavaStandardName()) {
                        LOG.debug("Computing {} hash of {}", fixityAlgorithm.getJavaStandardName(), file);
                        var digest = DigestUtil.computeDigest(fixityAlgorithm, file);
                        inventoryUpdater.addFixity(result.getContentPath(), fixityAlgorithm, digest);
                    }
                }
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
