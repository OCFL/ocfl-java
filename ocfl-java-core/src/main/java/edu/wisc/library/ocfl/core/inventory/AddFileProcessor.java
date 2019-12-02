package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.concurrent.ParallelProcess;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

/**
 * Encapsulates logic for adding files to an object, both adding them to the inventory and moving them into staging.
 */
public class AddFileProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(AddFileProcessor.class);

    private ParallelProcess parallelProcess;
    private ParallelProcess copyParallelProcess;

    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;
    private DigestAlgorithm digestAlgorithm;

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

        public AddFileProcessor build(InventoryUpdater inventoryUpdater, Path stagingDir, DigestAlgorithm digestAlgorithm) {
            return new AddFileProcessor(inventoryUpdater, stagingDir, digestAlgorithm, parallelProcess, copyParallelProcess);
        }

    }

    /**
     * @see Builder
     *
     * @param inventoryUpdater the inventory updater
     * @param stagingDir the staging directory to move files into
     * @param digestAlgorithm the digest algorithm
     * @param parallelProcess processor for calculating digests
     * @param copyParallelProcess processor for moving files
     */
    public AddFileProcessor(InventoryUpdater inventoryUpdater, Path stagingDir, DigestAlgorithm digestAlgorithm,
                            ParallelProcess parallelProcess, ParallelProcess copyParallelProcess) {
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.parallelProcess = Enforce.notNull(parallelProcess, "parallelProcess cannot be null");
        this.copyParallelProcess = Enforce.notNull(copyParallelProcess, "copyParallelProcess cannot be null");
    }

    /**
     * Adds all of the files at or under the sourcePath to the root of the object.
     *
     * @param sourcePath the file or directory to add
     * @param ocflOptions options for how to move the files
     * @return a map of logical paths to their corresponding file within the stagingDir for newly added files
     */
    public Map<String, Path> processPath(Path sourcePath, OcflOption... ocflOptions) {
        return processPath(sourcePath, "", ocflOptions);
    }

    /**
     * Adds all of the files at or under the sourcePath to the object at the specified destinationPath.
     *
     * @param sourcePath the file or directory to add
     * @param destinationPath the location to insert the file or directory at within the object
     * @param ocflOptions options for how to move the files
     * @return a map of logical paths to their corresponding file within the stagingDir for newly added files
     */
    public Map<String, Path> processPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions) {
        Enforce.notNull(sourcePath, "sourcePath cannot be null");
        Enforce.notNull(destinationPath, "destinationPath cannot be null");

        var results = new HashMap<String, Path>();
        var options = new HashSet<>(Arrays.asList(ocflOptions));

        var files = FileUtil.findFiles(sourcePath);

        if (files.size() == 0 && "".equals(destinationPath)) {
            // An empty putObject call -- do nothing
            return results;
        } else if (files.size() == 0) {
            throw new IllegalArgumentException(String.format("No files were found under %s to add", sourcePath));
        }

        var destination = destinationPath(destinationPath, sourcePath);

        var filesWithDigests = parallelProcess.collection(files, file -> {
            var digest = DigestUtil.computeDigest(digestAlgorithm, file);
            return Map.entry(file, digest);
        });

        var newFiles = new HashMap<Path, InventoryUpdater.AddFileResult>();

        filesWithDigests.forEach(entry -> {
            var file = entry.getKey();
            var digest = entry.getValue();

            var logicalPath = logicalPath(sourcePath, file, destination);
            var result = inventoryUpdater.addFile(digest, logicalPath, ocflOptions);

            if (result.isNew()) {
                newFiles.put(file, result);
                results.put(logicalPath, stagingFullPath(result.getPathUnderContentDir()));
            }
        });

        copyParallelProcess.map(newFiles, (file, result) -> {
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

        return results;
    }

    private String destinationPath(String path, Path sourcePath) {
        if (path.isBlank() && Files.isRegularFile(sourcePath)) {
            return sourcePath.getFileName().toString();
        }
        return path;
    }

    private String logicalPath(Path sourcePath, Path sourceFile, String destinationPath) {
        var sourceRelative = FileUtil.pathToStringStandardSeparator(sourcePath.relativize(sourceFile));
        return FileUtil.pathJoinIgnoreEmpty(destinationPath, sourceRelative);
    }

    private Path stagingFullPath(String pathUnderContentDir) {
        return Paths.get(FileUtil.pathJoinFailEmpty(stagingDir.toString(), pathUnderContentDir));
    }

}
