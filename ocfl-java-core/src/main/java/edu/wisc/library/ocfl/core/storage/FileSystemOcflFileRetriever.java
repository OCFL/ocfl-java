package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

/**
 * OcflFileRetriever implementation for lazy-loading files from file system storage.
 */
public class FileSystemOcflFileRetriever implements OcflFileRetriever {

    private final Path filePath;
    private final String digestAlgorithm;
    private final String digestValue;

    public FileSystemOcflFileRetriever(Path filePath, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.filePath = Enforce.notNull(filePath, "filePath cannot be null");
        Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.digestAlgorithm = digestAlgorithm.getJavaStandardName();
        this.digestValue = Enforce.notBlank(digestValue, "digestValue cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile() {
        try {
            return new FixityCheckInputStream(Files.newInputStream(filePath), digestAlgorithm, digestValue);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
