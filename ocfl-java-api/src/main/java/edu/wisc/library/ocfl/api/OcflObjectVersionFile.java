package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Map;

/**
 * Represents a file within an OCFL object at a specific version. The file content can be lazy-loaded.
 */
public class OcflObjectVersionFile {

    private FileDetails fileDetails;
    private OcflFileRetriever fileRetriever;

    public OcflObjectVersionFile(FileDetails fileDetails, OcflFileRetriever fileRetriever) {
        this.fileDetails = Enforce.notNull(fileDetails, "fileDetails cannot be null");
        this.fileRetriever = Enforce.notNull(fileRetriever, "fileRetriever cannot be null");
    }

    /**
     * The file's logical path within the object
     *
     * @return logical path
     */
    public String getPath() {
        return fileDetails.getPath();
    }

    /**
     * The file's path relative to the storage root
     *
     * @return storage relative path
     */
    public String getStorageRelativePath() {
        return fileDetails.getStorageRelativePath();
    }

    /**
     * Map of digest algorithm to digest value.
     *
     * @return digest map
     */
    public Map<DigestAlgorithm, String> getFixity() {
        return fileDetails.getFixity();
    }

    /**
     * Returns a new input stream of the file's content. The caller is responsible for closing the stream.
     *
     * <p>The caller may call {@code checkFixity()} on the InputStream after streaming all of that data to ensure the
     * fixity of data.
     *
     * @return FixityCheckInputStream of the file's content
     */
    public FixityCheckInputStream getStream() {
        return fileRetriever.retrieveFile();
    }

    @Override
    public String toString() {
        return "OcflObjectVersionFile{" +
                "fileDetails='" + fileDetails + '\'' +
                '}';
    }

}
