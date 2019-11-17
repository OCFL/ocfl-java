package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.model.VersionId;

import java.io.InputStream;
import java.nio.file.Path;

/**
 * Exposes methods for selectively updating a specific OCFL object.
 */
public interface OcflObjectUpdater {

    /**
     * Adds a file or directory to the object being operated on at the specified destinationPath.
     *
     * <p>By default, files are copied into the OCFL repository. If {@code OcflOption.MOVE_SOURCE} is specified, then
     * files will be moved instead. Warning: If an exception occurs and the new version is not created, the files that were
     * will be lost. This operation is more efficient but less safe than the default copy.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@code OcflOption.OVERWRITE}.
     *
     * @param sourcePath the local file or directory to add to the object
     * @param destinationPath the logical path to store the sourcePath at within the object, an empty string indicates the object root
     * @param ocflOptions optional config options. Use {@code OcflOption.MOVE_SOURCE} to move files into the repo instead of copying.
     *                    Use {@code OcflOption.OVERWRITE} to overwrite existing files within an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater addPath(Path sourcePath, String destinationPath, OcflOption... ocflOptions);

    /**
     * Writes the contents of the InputStream to the object being operated on at the specified destinationPath.
     *
     * <p>Pass a {@code FixityCheckInputStream} to ensure transmission fixity.
     *
     * <p>By default, the change will be rejected if there is already a file in the object at the destinationPath.
     * To overwrite, specify {@code OcflOption.OVERWRITE}.
     *
     * @param input InputStream containing the content of a file to add to an object
     * @param destinationPath the logical path to store the file at within the object
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     * @throws FixityCheckException if the a FixityCheckInputStream is used and the digest does not match the expected value
     */
    OcflObjectUpdater writeFile(InputStream input, String destinationPath, OcflOption... ocflOptions);

    /**
     * Removes a file from the object. An exception is not thrown if there is nothing at the path.
     *
     * @param path the logical path of the file to remove
     */
    OcflObjectUpdater removeFile(String path);

    /**
     * Renames an existing file within the object. Use {@code OcflOption.OVERWRITE} to overwrite an existing file at the destinationPath.
     *
     * @param sourcePath the logical path to the file to be renamed
     * @param destinationPath the local path to rename the file to
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater renameFile(String sourcePath, String destinationPath, OcflOption... ocflOptions);

    /**
     * Reinstates a file that existed in any version of the object into the current version. This is useful when recovering
     * a prior version of a file or adding back a file that was deleted. Use {@code OcflOption.OVERWRITE} to overwrite
     * an existing file at the destinationPath.
     *
     * @param sourceVersionId the version id of the version to reinstate the sourcePath from. Cannot be the current version
     * @param sourcePath the logical path to the file to be reinstated
     * @param destinationPath the logical path to reinstate the file to
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectUpdater reinstateFile(VersionId sourceVersionId, String sourcePath, String destinationPath, OcflOption... ocflOptions);

    // TODO add api for purging a file in an object

}
