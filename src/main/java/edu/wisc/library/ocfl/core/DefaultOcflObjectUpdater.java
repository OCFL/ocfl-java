package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.UpdateOption;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.FileUtil;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;

public class DefaultOcflObjectUpdater implements OcflObjectUpdater {

    private InventoryUpdater inventoryUpdater;
    private Path stagingDir;

    public DefaultOcflObjectUpdater(InventoryUpdater inventoryUpdater, Path stagingDir) {
        this.inventoryUpdater = Enforce.notNull(inventoryUpdater, "inventoryUpdater cannot be null");
        this.stagingDir = Enforce.notNull(stagingDir, "stagingDir cannot be null");
    }

    @Override
    public OcflObjectUpdater addPath(Path sourcePath, String destinationPath, UpdateOption... updateOptions) {
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
            var isNew = inventoryUpdater.addFile(file, stagingDir.relativize(stagingFullPath), updateOptions);
            if (isNew) {
                FileUtil.copyFileMakeParents(file, stagingFullPath);
            }
        });

        return this;
    }

    @Override
    public OcflObjectUpdater writeFile(InputStream input, String destinationPath, UpdateOption... updateOptions) {
        Enforce.notNull(input, "input cannot be null");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        var stagingDst = stagingDir.resolve(destinationPath);

        FileUtil.createDirectories(stagingDst.getParent());
        copyInputStream(input, stagingDst);

        var isNew = inventoryUpdater.addFile(stagingDst, stagingDir.relativize(stagingDst), updateOptions);
        if (!isNew) {
            delete(stagingDst);
        }

        return this;
    }

    @Override
    public OcflObjectUpdater removeFile(String path) {
        Enforce.notBlank(path, "path cannot be blank");

        inventoryUpdater.removeFile(path);

        return this;
    }

    @Override
    public OcflObjectUpdater renameFile(String sourcePath, String destinationPath, UpdateOption... updateOptions) {
        Enforce.notBlank(sourcePath, "sourcePath cannot be blank");
        Enforce.notBlank(destinationPath, "destinationPath cannot be blank");

        inventoryUpdater.renameFile(sourcePath, destinationPath, updateOptions);

        return this;
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

}
