package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.OverwriteException;
import edu.wisc.library.ocfl.api.model.VersionDetails;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;

/**
 * Exposes methods for selectively reading files from a specific object at a specific version.
 */
public interface OcflObjectReader {

    /**
     * Returns details about the composition of the object at the current version.
     *
     * @return details about the object version
     */
    VersionDetails describeVersion();

    /**
     * Returns a list of all of the paths, relative the object root, of all of the files within the object at the current
     * version.
     *
     * @return list of all files in an object
     */
    Collection<String> listFiles();

    /**
     * Writes a specific file to the given destinationPath. Use {@code OcflOption.OVERWRITE} to overwrite the destination
     * path if it already exists.
     *
     * @param sourcePath the object root relative path to the file to retrieve from the object
     * @param destinationPath the destination to write the file to
     * @param ocflOptions optional config options. Use {@code OcflOption.OVERWRITE} to overwrite existing files within
     *                    an object
     * @throws OverwriteException if there is already a file at the destinationPath and {@code OcflOption.OVERWRITE} was
     *                            not specified
     */
    OcflObjectReader getFile(String sourcePath, Path destinationPath, OcflOption... ocflOptions);

    /**
     * Retrieves the specified file and writes it to an {@code InputStream}.
     *
     * <p>Important: The stream can only be used within the OfclObjectReader block, and will be automatically closed when
     * it terminates.
     *
     * @param sourcePath the object root relative path to the file to retrieve from the object
     * @return InputStream containing the file's content
     */
    // TODO change this so that it returns a stream to the file in the repo
    InputStream getFile(String sourcePath);

    // TODO add an api that is able to lazy load inputstreams for any file in an object

}
