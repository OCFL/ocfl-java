package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.OverwriteException;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * Exposes methods for selectively updating a specific OCFL object.
 */
public interface OcflObjectUpdater {

    /**
     * Adds a file or directory to the object being operated on. The destinationPath is where the file is inserted
     * into the object relative to the object's root.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@code OcflOption.OVERWRITE}.
     *
     * @param sourcePath the local file or directory to add to the object
     * @param destinationPath the location to store the sourcePath at within the object
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions);

    /**
     * Writes the contents of the InpuStream to the object being operated on. The destinationPath is where the file is
     * inserted into the object relative to the object's root.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@code OcflOption.OVERWRITE}.
     *
     * @param input InputStream containing the content of a file to add to an object
     * @param destinationPath the location to store the file at within the object
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater writeFile(InputStream input, String destinationPath, OcflOption... ocflOptions);

    /**
     * Removes a file from the object. The given path should be relative to the object's root. An exception is not thrown
     * if there is nothing at the path.
     *
     * @param path the path to the file to remove
     */
    OcflObjectUpdater removeFile(String path);

    /**
     * Renames an existing file within the object. Both paths are relative the object's root. Use {@code OcflOption.OVERWRITE}
     * to overwrite an existing file at the destinationPath.
     *
     * @param sourcePath the path to the file to be renamed relative the object root
     * @param destinationPath the path to rename the file to relative the object root
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions);

    // TODO add api for reinstating a file from a previous version

    // TODO add api for purging a file an object

}
