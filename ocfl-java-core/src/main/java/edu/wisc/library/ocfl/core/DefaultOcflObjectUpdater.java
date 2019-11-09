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

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.DigestInputStream;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Default implementation of OcflObjectUpdater that is used by DefaultOcflRepository to provide write access to an object.
 * <p>
 * This class is NOT thread safe.
 */
public class DefaultOcflObjectUpdater implements OcflObjectUpdater {

    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;
    private ParallelProcess parallelProcess;
    private ParallelProcess copyParallelProcess;

    private Set<Path> newFiles;

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

        var normalized = normalizeDestinationPath(destinationPath);

        if (Files.isRegularFile(sourcePath) && "".equals(normalized.toString())) {
            throw new IllegalArgumentException("A destination path must be specified when adding a file.");
        }

        var stagingDst = stagingDir.resolve(normalized);
        var files = FileUtil.findFiles(sourcePath);

        if (files.size() == 0) {
            throw new IllegalArgumentException(String.format("No files were found under %s to add", sourcePath));
        }

        var filesWithDigests = parallelProcess.collection(files, file -> {
            var digest = inventoryUpdater.computeDigest(file);
            return Map.entry(file, digest);
        });

        var copyFiles = new HashSet<Path>();

        filesWithDigests.forEach(entry -> {
            var file = entry.getKey();
            var digest = entry.getValue();
            var stagingRelativePath = stagingRelativePath(sourcePath, stagingDst, file);
            var isNew = inventoryUpdater.addFile(digest, file, stagingRelativePath, ocflOptions);
            if (isNew) {
                copyFiles.add(file);
                newFiles.add(stagingRelativePath);
            }
        });

        copyParallelProcess.collection(copyFiles, file -> {
            var stagingFullPath = stagingFullPath(sourcePath, stagingDst, file);
            if (options.contains(OcflOption.MOVE_SOURCE)) {
                FileUtil.moveFileMakeParents(file, stagingFullPath);
            } else {
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

        var normalized = normalizeDestinationPath(destinationPath);
        var stagingDst = stagingDir.resolve(normalized);
        var stagingRelative = stagingDir.relativize(stagingDst);

        FileUtil.createDirectories(stagingDst.getParent());

        var digestInput = wrapInDigestInputStream(input);
        copyInputStream(digestInput, stagingDst);

        if (input instanceof FixityCheckInputStream) {
            ((FixityCheckInputStream) input).checkFixity();
        }

        var digest = Hex.encodeHexString(digestInput.getMessageDigest().digest());
        var isNew = inventoryUpdater.addFile(digest, stagingDst, stagingRelative, ocflOptions);
        if (!isNew) {
            FileUtil.delete(stagingDst);
        } else {
            newFiles.add(stagingRelative);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        var filePath = Paths.get(path);

        enforceNoMutationsOnNewFiles(filePath);

        inventoryUpdater.removeFile(filePath);

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        // TODO Revert these path conversions. They must be treated as opaque strings
        var sourceFilePath = Paths.get(sourcePath);

        enforceNoMutationsOnNewFiles(sourceFilePath);

        var normalizedDestination = normalizeDestinationPath(destinationPath);

        inventoryUpdater.renameFile(sourceFilePath, normalizedDestination, ocflOptions);

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

        var sourceFilePath = Paths.get(sourcePath);
        var normalizedDestination = normalizeDestinationPath(destinationPath);

        inventoryUpdater.reinstateFile(sourceVersionId, sourceFilePath, normalizedDestination, ocflOptions);

        return this;
    }

    private void enforceNoMutationsOnNewFiles(Path path) {
        if (newFiles.contains(path)) {
            throw new UnsupportedOperationException(String.format("File %s was added in the current version and cannot be mutated.", path));
        }
    }

    /*
     * This is necessary to ensure that the specified paths are contained within the object root
     */
    private Path normalizeDestinationPath(String destinationPath) {
        var normalized = Paths.get(destinationPath).normalize();
        validateDestinationPath(normalized);
        return normalized;
    }

    private void validateDestinationPath(Path destination) {
        if (destination.isAbsolute()) {
            throw new IllegalArgumentException(
                    String.format("Invalid destination %s. Path must be relative the object root.", destination));
        }

        if (destination.startsWith("..")) {
            throw new IllegalArgumentException(
                    String.format("Invalid destination %s. Path cannot be outside of object root.", destination));
        }
    }

    private Path stagingFullPath(Path sourceRoot, Path destinationRoot, Path file) {
        var sourceRelative = sourceRoot.relativize(file);
        return destinationRoot.resolve(sourceRelative);
    }

    private Path stagingRelativePath(Path sourceRoot, Path destinationRoot, Path file) {
        var stagingFullPath = stagingFullPath(sourceRoot, destinationRoot, file);
        return stagingDir.relativize(stagingFullPath);
    }

    private DigestInputStream wrapInDigestInputStream(InputStream input) {
        if (input instanceof FixityCheckInputStream) {
            var digestAlgorithm = ((FixityCheckInputStream) input).getMessageDigest().getAlgorithm();
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
