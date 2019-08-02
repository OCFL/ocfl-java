package edu.wisc.library.ocfl.api;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Collection;

public interface OcflObjectReader {

    // TODO describe

    Collection<String> listFiles();

    void getFile(String sourcePath, Path destinationPath);

    /**
     * Retrieves the specified file and returns it on an InputStream.
     *
     * Important: The stream can only be used within the OfclObjectReader block, and will be automatically closed when
     * it terminates.
     *
     * @param sourcePath
     * @return
     */
    InputStream getFile(String sourcePath);

}
