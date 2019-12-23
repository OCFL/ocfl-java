package edu.wisc.library.ocfl.core.storage.cloud;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.nio.file.Path;

/**
 * Builder for constructing S3OcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new S3OcflStorageBuilder().s3Client(s3Client).workDir(workDir).build(bucketName).}
 */
public class CloudOcflStorageBuilder {

    private int threadPoolSize;
    private ObjectMapper objectMapper;
    private CloudClient cloudClient;
    private CloudOcflStorageInitializer initializer;
    private Path workDir;

    public CloudOcflStorageBuilder() {
        this.threadPoolSize = Runtime.getRuntime().availableProcessors();
        this.objectMapper = ObjectMappers.prettyPrintMapper();
    }

    /**
     * Sets the cloud client. This must be set prior to calling build().
     *
     * @param cloudClient the client to use to interface with cloud storage such as S3
     * @return builder
     */
    public CloudOcflStorageBuilder cloudClient(CloudClient cloudClient) {
        this.cloudClient = cloudClient;
        return this;
    }

    /**
     * Set the directory to write temporary files to. This must be set prior to calling build().
     *
     * @param workDir the directory to write temporary files to.
     * @return builder
     */
    public CloudOcflStorageBuilder workDir(Path workDir) {
        this.workDir = workDir;
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public CloudOcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default thread pool size. Default: the number of available processors
     *
     * @param threadPoolSize thread pool size. Default: number of processors
     * @return builder
     */
    public CloudOcflStorageBuilder threadPoolSize(int threadPoolSize) {
        this.threadPoolSize = Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");
        return this;
    }

    /**
     * Overrides the default {@link CloudOcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public CloudOcflStorageBuilder initializer(CloudOcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * Builds a new {@link CloudOcflStorage} object
     */
    public CloudOcflStorage build() {
        var init = initializer;
        if (init == null) {
            init = new CloudOcflStorageInitializer(cloudClient, objectMapper, new ObjectIdPathMapperBuilder());
        }

        return new CloudOcflStorage(cloudClient, threadPoolSize, workDir, init);
    }

}
