package edu.wisc.library.ocfl.aws;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;

/**
 * OcflFileRetriever implementation for lazy-loading files from S3 storage.
 */
public class S3OcflFileRetriever implements OcflFileRetriever {

    private final S3Client s3Client;
    private final String bucket;
    private final String key;
    private final String digestAlgorithm;
    private final String digestValue;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private S3Client s3Client;
        private String bucket;

        public Builder s3Client(S3Client s3Client) {
            this.s3Client = s3Client;
            return this;
        }

        public Builder bucket(String bucket) {
            this.bucket = bucket;
            return this;
        }

        public S3OcflFileRetriever build(String key, DigestAlgorithm digestAlgorithm, String digestValue) {
            return new S3OcflFileRetriever(s3Client, bucket, key, digestAlgorithm, digestValue);
        }

    }

    public S3OcflFileRetriever(S3Client s3Client, String bucket, String key, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.s3Client = Enforce.notNull(s3Client, "s3Client cannot be null");
        this.bucket = Enforce.notBlank(bucket, "bucket cannot be blank");
        this.key = Enforce.notBlank(key, "key cannot be blank");
        Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        this.digestAlgorithm = digestAlgorithm.getJavaStandardName();
        this.digestValue = Enforce.notBlank(digestValue, "digestValue cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public FixityCheckInputStream retrieveFile() {
        // TODO caching?
        return new FixityCheckInputStream(s3Client.getObject(GetObjectRequest.builder()
                .bucket(bucket)
                .key(key)
                .build()), digestAlgorithm, digestValue);
    }

}
