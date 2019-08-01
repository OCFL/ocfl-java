package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.nio.file.Path;

public class DefaultOcflObjectUpdater implements OcflObjectUpdater {

    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;

    public DefaultOcflObjectUpdater(InventoryUpdater inventoryUpdater, Path stagingDir) {
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
    }

    @Override
    public OcflObjectUpdater addPath(Path sourcePath, String destinationPath) {
        Enforce.notNull(sourcePath, "sourcePath cannot be null");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var stagingDst = stagingDir.resolve(destinationPath);

        var files = FileUtil.findFiles(sourcePath);

        if (files.size() == 0) {
            throw new IllegalArgumentException(String.format("No files were found under %s to add", sourcePath));
        }

        files.forEach(file -> {
            var sourceRelative = sourcePath.relativize(file);
            var stagingFullPath = stagingDst.resolve(sourceRelative);
            var isNew = inventoryUpdater.addFile(file, stagingDir.relativize(stagingFullPath));
            if (isNew) {
                FileUtil.copyFileMakeParents(file, stagingFullPath);
            }
        });

        return this;
    }

    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        inventoryUpdater.removeFile(path);

        return this;
    }

    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        inventoryUpdater.renameFile(sourcePath, destinationPath);

        return this;
    }

}
