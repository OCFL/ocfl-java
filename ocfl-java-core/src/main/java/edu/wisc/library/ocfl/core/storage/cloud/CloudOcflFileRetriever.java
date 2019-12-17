package edu.wisc.library.ocfl.core.storage.cloud;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;

/**
 * OcflFileRetriever implementation for lazy-loading files from cloud storage.
 */
public class CloudOcflFileRetriever implements OcflFileRetriever {

    private final CloudClient cloudClient;
    private final String key;
    private final DigestAlgorithm digestAlgorithm;
    private final String digestValue;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private CloudClient cloudClient;

        public Builder cloudClient(CloudClient cloudClient) {
            this.cloudClient = cloudClient;
            return this;
        }

        public CloudOcflFileRetriever build(String key, DigestAlgorithm digestAlgorithm, String digestValue) {
            return new CloudOcflFileRetriever(cloudClient, key, digestAlgorithm, digestValue);
        }

    }

    public CloudOcflFileRetriever(CloudClient cloudClient, String key, DigestAlgorithm digestAlgorithm, String digestValue) {
        this.cloudClient = Enforce.notNull(cloudClient, "cloudClient cannot be null");
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
        return new FixityCheckInputStream(cloudClient.downloadStream(key), digestAlgorithm, digestValue);
    }

}
