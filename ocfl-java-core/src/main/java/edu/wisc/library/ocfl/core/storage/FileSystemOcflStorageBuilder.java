package edu.wisc.library.ocfl.core.storage;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;

import java.nio.file.Path;

/**
 * Builder for constructing FileSystemOcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code new FileSystemOcflStorageBuilder().build(repoPath, idMapper).}
 */
public class FileSystemOcflStorageBuilder {

    private InventoryMapper inventoryMapper;
    private int threadPoolSize;
    private boolean checkNewVersionFixity;
    private ObjectMapper objectMapper;

    public FileSystemOcflStorageBuilder() {
        this.inventoryMapper = InventoryMapper.defaultMapper();
        this.threadPoolSize = Runtime.getRuntime().availableProcessors();
        this.checkNewVersionFixity = false;
        this.objectMapper = new ObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    }

    /**
     * Overrides the default InventoryMapper that's used for parsing inventory files.
     *
     * @param inventoryMapper the mapper that's used to parse inventory files
     */
    public FileSystemOcflStorageBuilder inventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     */
    public FileSystemOcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default thread pool size. Default: the number of available processors
     *
     * @param threadPoolSize thread pool size. Default: number of processors
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
     */
    public FileSystemOcflStorageBuilder checkNewVersionFixity(boolean checkNewVersionFixity) {
        this.checkNewVersionFixity = checkNewVersionFixity;
        return this;
    }

    /**
     * Builds a new FileSystemOcflStorage object
     *
     * @param repositoryRoot the path to the OCFL storage root
     * @param objectIdPathMapper the ObjectIdPathMapper used to map object ids to storage paths
     */
    public FileSystemOcflStorage build(Path repositoryRoot, ObjectIdPathMapper objectIdPathMapper) {
        return new FileSystemOcflStorage(repositoryRoot, objectIdPathMapper, threadPoolSize,
                checkNewVersionFixity, inventoryMapper, objectMapper);
    }

}
