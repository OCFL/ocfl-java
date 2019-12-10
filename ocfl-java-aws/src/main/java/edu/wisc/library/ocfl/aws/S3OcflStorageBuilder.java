package edu.wisc.library.ocfl.aws;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.util.ObjectMappers;
import software.amazon.awssdk.services.s3.S3Client;

import java.nio.file.Path;

/**
 * Builder for constructing S3OcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new S3OcflStorageBuilder().s3Client(s3Client).workDir(workDir).build(bucketName).}
 */
public class S3OcflStorageBuilder {

    private InventoryMapper inventoryMapper;
    private int threadPoolSize;
    private ObjectMapper objectMapper;
    private S3Client s3Client;
    private S3OcflStorageInitializer initializer;
    private Path workDir;

    public S3OcflStorageBuilder() {
        this.inventoryMapper = InventoryMapper.defaultMapper();
        this.threadPoolSize = Runtime.getRuntime().availableProcessors();
        this.objectMapper = ObjectMappers.prettyPrintMapper();
    }

    /**
     * Sets the S3 client. This must be set prior to calling build().
     *
     * @param s3Client the client to use to interface with S3
     * @return builder
     */
    public S3OcflStorageBuilder s3Client(S3Client s3Client) {
        this.s3Client = s3Client;
        return this;
    }

    /**
     * Set the directory to write temporary files to. This must be set prior to calling build().
     *
     * @param workDir the directory to write temporary files to.
     * @return builder
     */
    public S3OcflStorageBuilder workDir(Path workDir) {
        this.workDir = workDir;
        return this;
    }

    /**
     * Overrides the default InventoryMapper that's used for parsing inventory files.
     *
     * @param inventoryMapper the mapper that's used to parse inventory files
     * @return builder
     */
    public S3OcflStorageBuilder inventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public S3OcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default thread pool size. Default: the number of available processors
     *
     * @param threadPoolSize thread pool size. Default: number of processors
     * @return builder
     */
    public S3OcflStorageBuilder threadPoolSize(int threadPoolSize) {
        this.threadPoolSize = Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");
        return this;
    }

    /**
     * Overrides the default {@link S3OcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public S3OcflStorageBuilder initializer(S3OcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * Builds a new S3OcflStorage object
     *
     * @param bucketName the bucket the OCFL repository is in
     */
    public S3OcflStorage build(String bucketName) {
        var init = initializer;
        if (init == null) {
            init = new S3OcflStorageInitializer(s3Client, objectMapper, new ObjectIdPathMapperBuilder());
        }

        return new S3OcflStorage(s3Client, bucketName, threadPoolSize, workDir, inventoryMapper, init);
    }

}
