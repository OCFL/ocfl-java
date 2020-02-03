package edu.wisc.library.ocfl.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabase;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabaseBuilder;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.lock.InMemoryObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLockBuilder;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.DefaultContentPathConstraints;
import edu.wisc.library.ocfl.core.path.sanitize.NoOpPathSanitizer;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.storage.CachingOcflStorage;
import edu.wisc.library.ocfl.core.storage.ObjectDetailsDbOcflStorage;
import edu.wisc.library.ocfl.core.storage.OcflStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Constructs a local file system based OCFL repository sensible defaults that can be overriden prior to calling
 * build().
 *
 * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
 */
public class OcflRepositoryBuilder {

    private OcflStorage storage;
    private OcflConfig config;
    private LayoutConfig layoutConfig;
    private Path workDir;

    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private InventoryMapper inventoryMapper;
    private PathSanitizer pathSanitizer;
    private ContentPathConstraintProcessor contentPathConstraintProcessor;
    private ObjectDetailsDatabase objectDetailsDb;

    private int digestThreadPoolSize;
    private int copyThreadPoolSize;

    /**
     * Constructs a local file system based OCFL repository sensible defaults that can be overriden prior to calling
     * build().
     *
     * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
     */
    public OcflRepositoryBuilder() {
        config = new OcflConfig();
        objectLock = new InMemoryObjectLock(10, TimeUnit.SECONDS);
        inventoryCache = new CaffeineCache<>(Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .expireAfterAccess(Duration.ofMinutes(10))
                .maximumSize(1_000).build());
        inventoryMapper = InventoryMapper.defaultMapper();
        pathSanitizer = new NoOpPathSanitizer();
        contentPathConstraintProcessor = DefaultContentPathConstraints.none();
        digestThreadPoolSize = Runtime.getRuntime().availableProcessors();
        copyThreadPoolSize = digestThreadPoolSize * 2;
    }

    /**
     * The storage layer the repository should use. Required.
     *
     * @param storage the storage layer implementation that the OCFL repository should use
     * @return builder
     */
    public OcflRepositoryBuilder storage(OcflStorage storage) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        return this;
    }

    /**
     * The temporary workspace the repository uses to assemble object versions. This directory cannot be located within
     * the OCFL storage root. Required.
     *
     * @param workDir the work directory to assemble versions in before they're moved to storage -- cannot be within the OCFL storage root
     * @return builder
     */
    public OcflRepositoryBuilder workDir(Path workDir) {
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        return this;
    }

    /**
     * Used to lock objects when writing. The default is an {@link InMemoryObjectLock} instance that will wait 10 seconds
     * for the lock before failing. Override to change the wait period or implement a different type of lock.
     *
     * <p>Use {@link ObjectLockBuilder} to construct an object lock that's backed by a relational database. This
     * is primarily intended to be used when working with a cloud object store like S3.
     *
     * @param objectLock object lock
     * @return builder
     * @see ObjectLockBuilder
     */
    public OcflRepositoryBuilder objectLock(ObjectLock objectLock) {
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        return this;
    }

    /**
     * Used to cache deserialized inventories. The default is an in memory {@link CaffeineCache} instance that has a maximum size
     * of 1,000 objects and an expiry of 10 minutes. Override to adjust the settings or change the cache implementation.
     *
     * @param inventoryCache inventory cache
     * @return builder
     */
    public OcflRepositoryBuilder inventoryCache(Cache<String, Inventory> inventoryCache) {
        this.inventoryCache = inventoryCache;
        return this;
    }

    /**
     * Used to store details about OCFL objects in the repository. This is primarily intended to be used when working
     * with a cloud object store like S3. Use {@link ObjectDetailsDatabaseBuilder} to construct an {@link ObjectDetailsDatabase}
     * instance.
     *
     * @param objectDetailsDb object details db
     * @return builder
     * @see ObjectDetailsDatabaseBuilder
     */
    public OcflRepositoryBuilder objectDetailsDb(ObjectDetailsDatabase objectDetailsDb) {
        this.objectDetailsDb = objectDetailsDb;
        return this;
    }

    /**
     * Changes the InventoryMapper to pretty print Inventory JSON files so that they are human readable but use more
     * disk space.
     *
     * @return builder
     */
    public OcflRepositoryBuilder prettyPrintJson() {
        return inventoryMapper(InventoryMapper.prettyPrintMapper());
    }

    /**
     * Used to override the default InventoryMapper, which is used to serialize Inventories to JSON files. The default
     * mapper will emit as little whitespace as possible.
     *
     * @param inventoryMapper inventory serializer
     * @return builder
     */
    public OcflRepositoryBuilder inventoryMapper(InventoryMapper inventoryMapper) {
        this.inventoryMapper = Enforce.notNull(inventoryMapper, "inventoryMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default NoOpPathSanitizer. PathSanitizers are used to clean logical file paths so that they can
     * safely be used as content paths to store files on disk.
     *
     * @param pathSanitizer path sanitizer
     * @return builder
     */
    public OcflRepositoryBuilder pathSanitizer(PathSanitizer pathSanitizer) {
        this.pathSanitizer = Enforce.notNull(pathSanitizer, "pathSanitizer cannot be null");
        return this;
    }

    /**
     * Overrides the default ContentPathConstraintProcessor that is used to enforce restrictions on what constitutes a valid
     * content path. By default, there are no restrictions.
     *
     * <p>Path constraints are applied AFTER the logical path has been sanitized, and are used to attempt to
     * ensure the portability of content paths. The following default generic constraint configurations are provided:
     *
     * <ul>
     *     <li>{@link DefaultContentPathConstraints#unix()}</li>
     *     <li>{@link DefaultContentPathConstraints#windows()}</li>
     *     <li>{@link DefaultContentPathConstraints#cloud()}</li>
     *     <li>{@link DefaultContentPathConstraints#all()}</li>
     *     <li>{@link DefaultContentPathConstraints#none()}</li>
     * </ul>
     *
     * <p>Constraints should be applied that target filesystems that are NOT the local filesystem. The local filesystem
     * will enforce its own constraints just fine. This mechanism is intended to enforce path constraints that the local
     * filesystem does not.
     *
     * <p>The following constraints are ALWAYS applied:
     *
     * <ul>
     *     <li>Cannot have a trailing /</li>
     *     <li>Cannot contain the following filenames: '.', '..'</li>
     *     <li>Cannot contain an empty filename</li>
     *     <li>Windows only: Cannot contain a \</li>
     * </ul>
     *
     * @param contentPathConstraintProcessor constraint processor
     * @return builder
     * @see DefaultContentPathConstraints
     */
    public OcflRepositoryBuilder contentPathConstraintProcessor(ContentPathConstraintProcessor contentPathConstraintProcessor) {
        this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
        return this;
    }

    /**
     * Sets OCFL configuration options.
     *
     * @param config ocfl config
     * @return builder
     */
    public OcflRepositoryBuilder ocflConfig(OcflConfig config) {
        this.config = Enforce.notNull(config, "config cannot be null");
        return this;
    }

    /**
     * Sets OCFL storage layout configuration. If no layout config is specified, the client will attempt to auto-detect
     * the configuration from an existing repository. If it is a new repository, then layout configuration MUST be supplied.
     *
     * @see edu.wisc.library.ocfl.core.extension.layout.config.DefaultLayoutConfig
     *
     * @param layoutConfig storage layout configuration
     * @return builder
     */
    public OcflRepositoryBuilder layoutConfig(LayoutConfig layoutConfig) {
        this.layoutConfig = Enforce.notNull(layoutConfig, "layoutConfig cannot be null");
        return this;
    }

    /**
     * Sets the size of the thread pool that's used to calculate digests. Default: the number of available processors.
     *
     * @param digestThreadPoolSize digest thread pool size
     * @return builder
     */
    public OcflRepositoryBuilder digestThreadPoolSize(int digestThreadPoolSize) {
        this.digestThreadPoolSize = Enforce.expressionTrue(digestThreadPoolSize > 0, digestThreadPoolSize, "digestThreadPoolSize must be greater than 0");
        return this;
    }

    /**
     * Sets the size of the thread pool that's used to move files around. Default: the number of available processors * 2.
     *
     * @param copyThreadPoolSize copy thread pool size
     * @return builder
     */
    public OcflRepositoryBuilder copyThreadPoolSize(int copyThreadPoolSize) {
        this.copyThreadPoolSize = Enforce.expressionTrue(copyThreadPoolSize > 0, copyThreadPoolSize, "copyThreadPoolSize must be greater than 0");
        return this;
    }

    /**
     * Constructs an OCFL repository. Brand new repositories are initialized.
     *
     * @return OcflRepository
     */
    public OcflRepository build() {
        return buildInternal(DefaultOcflRepository.class);
    }

    /**
     * Constructs an OCFL repository that allows the use of the Mutable HEAD Extension. Brand new repositories are initialized.
     *
     * @return MutableOcflRepository
     */
    public MutableOcflRepository buildMutable() {
        return buildInternal(DefaultMutableOcflRepository.class);
    }

    private <T extends OcflRepository> T buildInternal(Class<T> clazz) {
        Enforce.notNull(storage, "storage cannot be null");
        Enforce.notNull(workDir, "workDir cannot be null");

        var wrappedStorage = cache(db(storage));
        wrappedStorage.initializeStorage(config.getOcflVersion(), layoutConfig, inventoryMapper);

        Enforce.expressionTrue(Files.exists(workDir), workDir, "workDir must exist");
        Enforce.expressionTrue(Files.isDirectory(workDir), workDir, "workDir must be a directory");

        if (MutableOcflRepository.class.isAssignableFrom(clazz)) {
            return clazz.cast(new DefaultMutableOcflRepository(wrappedStorage, workDir,
                    objectLock, inventoryMapper,
                    pathSanitizer, contentPathConstraintProcessor,
                    config, digestThreadPoolSize, copyThreadPoolSize));
        }

        return clazz.cast(new DefaultOcflRepository(wrappedStorage, workDir,
                objectLock, inventoryMapper,
                pathSanitizer, contentPathConstraintProcessor,
                config, digestThreadPoolSize, copyThreadPoolSize));
    }

    private OcflStorage cache(OcflStorage storage) {
        if (inventoryCache != null) {
            return new CachingOcflStorage(inventoryCache, storage);
        }
        return storage;
    }

    private OcflStorage db(OcflStorage storage) {
        if (objectDetailsDb != null) {
            return new ObjectDetailsDbOcflStorage(objectDetailsDb, storage);
        }
        return storage;
    }

}
