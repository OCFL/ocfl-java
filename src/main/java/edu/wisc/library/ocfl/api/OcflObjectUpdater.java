package edu.wisc.library.ocfl.api;

import java.io.InputStream;
import java.nio.file.Path;

public interface OcflObjectUpdater {

    /**
     * Adds a file or directory to the object being operated on. The destinationPath is where the file should be inserted
     * into the object relative to the object's root.
     *
     * @param sourcePath
     * @param destinationPath
     * @return
     */
    OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions);

    /**
     * Write a file to the destinationPath that contains the contents of the given input stream.
     *
     * @param input
     * @param destinationPath
     * @param ocflOptions
     * @return
     */
    OcflObjectUpdater writeFile(InputStream input, String destinationPath, OcflOption... ocflOptions);

    /**
     * Removes a file from an object. The given path should be relative to the object's root.
     *
     * @param path
     * @return
     */
    OcflObjectUpdater removeFile(String path);

    /**
     * Renames an existing file within an object. Both paths are relative the object's root.
     *
     * @param sourcePath
     * @param destinationPath
     * @return
     */
    OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions);

}
