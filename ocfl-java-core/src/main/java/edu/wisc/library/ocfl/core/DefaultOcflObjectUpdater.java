package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.model.VersionId;
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
            var sourceRelative = sourcePath.relativize(file);
            var stagingFullPath = stagingDst.resolve(sourceRelative);
            var stagingRelative = stagingDir.relativize(stagingFullPath);
            var isNew = inventoryUpdater.addFile(digest, file, stagingRelative, ocflOptions);
            if (isNew) {
                copyFiles.add(file);
                newFiles.add(stagingRelative.toString());
            }
        });

        copyParallelProcess.collection(copyFiles, file -> {
            var sourceRelative = sourcePath.relativize(file);
            var stagingFullPath = stagingDst.resolve(sourceRelative);
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
        var digestInput = new DigestInputStream(input, inventoryUpdater.digestAlgorithm().getMessageDigest());
        copyInputStream(digestInput, stagingDst);

        // TODO add some tests of this
        if (input instanceof FixityCheckInputStream) {
            ((FixityCheckInputStream) input).checkFixity();
        }

        var digest = Hex.encodeHexString(digestInput.getMessageDigest().digest());
        var isNew = inventoryUpdater.addFile(digest, stagingDst, stagingRelative, ocflOptions);
        if (!isNew) {
            delete(stagingDst);
        } else {
            newFiles.add(stagingRelative.toString());
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        inventoryUpdater.removeFile(path);

        if (newFiles.remove(path)) {
            inventoryUpdater.removeFileFromManifest(path);
            delete(stagingDir.resolve(path));
            cleanupEmptyDirs(stagingDir);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var normalizedDestination = normalizeDestinationPath(destinationPath).toString();

        if (!newFiles.remove(sourcePath)) {
            inventoryUpdater.renameFile(sourcePath, normalizedDestination, ocflOptions);
        } else {
            // TODO Things get complicated when new-to-version files are mutated. Perhaps this should just not be allowed.
            newFiles.add(normalizedDestination);
            var destination = stagingDir.resolve(normalizedDestination);
            moveFile(stagingDir.resolve(sourcePath), destination);
            cleanupEmptyDirs(stagingDir);
            inventoryUpdater.removeFile(sourcePath);
            inventoryUpdater.removeFileFromManifest(sourcePath);
            var digest = inventoryUpdater.computeDigest(destination);
            inventoryUpdater.addFile(digest, destination, stagingDir.relativize(destination), ocflOptions);
        }

        return this;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectUpdater reinstateFile(String sourceVersionId, String sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notBlank(sourceVersionId, "sourceVersionId cannot be blank");
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var normalizedDestination = normalizeDestinationPath(destinationPath).toString();

        inventoryUpdater.reinstateFile(VersionId.fromValue(sourceVersionId), sourcePath, normalizedDestination, ocflOptions);

        return this;
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

    private void copyInputStream(InputStream input, Path dst) {
        try {
            Files.copy(input, dst);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void moveFile(Path source, Path destination) {
        try {
            Files.createDirectories(destination.getParent());
            Files.move(source, destination);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void cleanupEmptyDirs(Path root) {
        try (var files = Files.walk(root)) {
            files.filter(Files::isDirectory)
                    .filter(f -> !f.equals(root))
                    .filter(f -> f.toFile().list().length == 0)
                    .forEach(this::delete);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
