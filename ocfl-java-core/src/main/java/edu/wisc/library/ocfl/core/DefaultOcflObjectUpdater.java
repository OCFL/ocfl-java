package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.inventory.InventoryUpdater;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.apache.commons.codec.binary.Hex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.util.*;

/**
 * Default implementation of OcflObjectUpdater that is used by DefaultOcflRepository to provide write access to an object.
 * <p>
 * This class is NOT thread safe.
 */
public class DefaultOcflObjectUpdater implements OcflObjectUpdater {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectUpdater.class);

    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;
    private ParallelProcess parallelProcess;
    private ParallelProcess copyParallelProcess;

    private Set<String> newFiles;

    public DefaultOcflObjectUpdater(InventoryUpdater inventoryUpdater, Path stagingDir, ParallelProcess parallelProcess, ParallelProcess copyParallelProcess) {
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
        this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
        this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");
        newFiles = new HashSet<>();
    }

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

        var filesWithDigests = parallelProcess.collection(files, file -> {
            var digest = inventoryUpdater.computeDigest(file);
            return Map.entry(file, digest);
        });

        var copyFiles = new HashMap<Path, String>();

        filesWithDigests.forEach(entry -> {
            var file = entry.getKey();
            var digest = entry.getValue();
            var logicalPath = logicalPath(sourcePath, file, destinationPath);
            var result = inventoryUpdater.addFile(digest, file, logicalPath, ocflOptions);
            if (result.isNew()) {
                copyFiles.put(file, result.getPathUnderContentDir());
                newFiles.add(logicalPath);
            }
        });

        copyParallelProcess.map(copyFiles, (file, pathUnderContentDir) -> {
            var stagingFullPath = stagingFullPath(pathUnderContentDir);

            // TODO this is not currently overwritting existing files, which means that it's not possible to add a file
            // TODO with the same logical path multiple times within the same update block. Should it be?
            if (options.contains(OcflOption.MOVE_SOURCE)) {
                LOG.debug("Moving file <{}> to <{}>", file, stagingFullPath);
                FileUtil.moveFileMakeParents(file, stagingFullPath);
            } else {
                LOG.debug("Copying file <{}> to <{}>", file, stagingFullPath);
                FileUtil.copyFileMakeParents(file, stagingFullPath);
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
        var result = inventoryUpdater.addFile(digest, tempPath, destinationPath, ocflOptions);

        if (!result.isNew()) {
            LOG.debug("Deleting file <{}> because a file with same digest <{}> is already present in the object", tempPath, digest);
            FileUtil.delete(tempPath);
        } else {
            var stagingFullPath = stagingFullPath(result.getPathUnderContentDir());
            LOG.debug("Moving file <{}> to <{}>", tempPath, stagingFullPath);
            FileUtil.moveFileMakeParents(tempPath, stagingFullPath);
            newFiles.add(destinationPath);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        enforceNoMutationsOnNewFiles(path);
        inventoryUpdater.removeFile(path);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        enforceNoMutationsOnNewFiles(sourcePath);
        enforceNoMutationsOnNewFiles(destinationPath);

        inventoryUpdater.renameFile(sourcePath, destinationPath, ocflOptions);

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

        enforceNoMutationsOnNewFiles(destinationPath);

        inventoryUpdater.reinstateFile(sourceVersionId, sourcePath, destinationPath, ocflOptions);

        return this;
    }

    // TODO Think about changing the way versions are built so that this might be easier to support
    private void enforceNoMutationsOnNewFiles(String path) {
        if (newFiles.contains(path)) {
            throw new UnsupportedOperationException(String.format("File %s was added in the current version and cannot be mutated.", path));
        }
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
            if (inventoryUpdater.digestAlgorithm().getJavaStandardName().equalsIgnoreCase(digestAlgorithm)) {
                return (DigestInputStream) input;
            }
        }

        return new DigestInputStream(input, inventoryUpdater.digestAlgorithm().getMessageDigest());
    }

    private void copyInputStream(InputStream input, Path dst) {
        try {
            Files.copy(input, dst);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
