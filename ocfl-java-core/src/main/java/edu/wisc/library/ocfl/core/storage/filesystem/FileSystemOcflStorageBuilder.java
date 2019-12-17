package edu.wisc.library.ocfl.core.storage.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapperBuilder;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.nio.file.Path;

/**
 * Builder for constructing FileSystemOcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new FileSystemOcflStorageBuilder().build(repoPath).}
 */
public class FileSystemOcflStorageBuilder {

    private InventoryMapper inventoryMapper;
    private int threadPoolSize;
    private boolean checkNewVersionFixity;
    private ObjectMapper objectMapper;
    private FileSystemOcflStorageInitializer initializer;

    public FileSystemOcflStorageBuilder() {
        this.inventoryMapper = InventoryMapper.defaultMapper();
        this.threadPoolSize = Runtime.getRuntime().availableProcessors();
        this.checkNewVersionFixity = false;
        this.objectMapper = ObjectMappers.prettyPrintMapper();
    }

    /**
     * Overrides the default InventoryMapper that's used for parsing inventory files.
     *
     * @param inventoryMapper the mapper that's used to parse inventory files
     * @return builder
     */
    public FileSystemOcflStorageBuilder inventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public FileSystemOcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default thread pool size. Default: the number of available processors
     *
     * @param threadPoolSize thread pool size. Default: number of processors
     * @return builder
     */
    public FileSystemOcflStorageBuilder threadPoolSize(int threadPoolSize) {
        this.threadPoolSize = Enforce.expressionTrue(threadPoolSize > 0, threadPoolSize, "threadPoolSize must be greater than 0");
        return this;
    }

    /**
     * Overrides whether the fixity of new version content should be checked on version creation after moving the version
     * into the OCFL object root. Unless the work directory is on a different volume, it is unlikely that this check
     * is needed. Default: false
     *
     * @param checkNewVersionFixity whether to check fixity on version creation. Default: false
     * @return builder
     */
    public FileSystemOcflStorageBuilder checkNewVersionFixity(boolean checkNewVersionFixity) {
        this.checkNewVersionFixity = checkNewVersionFixity;
        return this;
    }

    /**
     * Overrides the default {@link FileSystemOcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public FileSystemOcflStorageBuilder initializer(FileSystemOcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * Builds a new FileSystemOcflStorage object
     *
     * @param repositoryRoot the path to the OCFL storage root
     */
    public FileSystemOcflStorage build(Path repositoryRoot) {
        var init = initializer;
        if (init == null) {
            init = new FileSystemOcflStorageInitializer(objectMapper, new ObjectIdPathMapperBuilder());
        }

        return new FileSystemOcflStorage(repositoryRoot, threadPoolSize, checkNewVersionFixity, inventoryMapper, init);
    }

}
