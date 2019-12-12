package edu.wisc.library.ocfl.aws;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

/**
 * OcflFileRetriever implementation for lazy-loading files from S3 storage.
 */
public class S3OcflFileRetriever implements OcflFileRetriever {

    private final S3ClientWrapper s3Client;
    private final String key;
    private final DigestAlgorithm digestAlgorithm;
    private final String digestValue;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private S3ClientWrapper s3Client;

        public Builder s3Client(S3ClientWrapper s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public S3OcflFileRetriever build(String key, DigestAlgorithm digestAlgorithm, String digestValue) {
            return new S3OcflFileRetriever(s3Client, key, digestAlgorithm, digestValue);
        }

    }

    public S3OcflFileRetriever(S3ClientWrapper s3Client, String key, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.key = Enforce.notBlank(key, "key cannot be blank");
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.digestValue = Enforce.notBlank(digestValue, "digestValue cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile() {
        // TODO caching?
        return new FixityCheckInputStream(s3Client.downloadStream(key), digestAlgorithm, digestValue);
    }

}
