package edu.wisc.library.ocfl.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.lock.InMemoryObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.DefaultContentPathConstraints;
import edu.wisc.library.ocfl.core.path.sanitize.NoOpPathSanitizer;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
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

    private OcflConfig config;

    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private InventoryMapper inventoryMapper;
    private PathSanitizer pathSanitizer;
    private ContentPathConstraintProcessor contentPathConstraintProcessor;

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
     * Used to lock objects when read and writing. The default is an InMemoryObjectLock instance that will wait 10 seconds
     * for the lock before failing. Override to change the wait period or implement a different type of lock, such as
     * a distributed lock when coordinating across multiple instances accessing the same OCFL repository.
     *
     * @param objectLock object lock
     * @return builder
     */
    public OcflRepositoryBuilder objectLock(ObjectLock objectLock) {
        this.objectLock = Enforce.notNull(objectLock, "objectLock cannot be null");
        return this;
    }

    /**
     * Used to cache deserialized inventories. The default is an in memory CaffeineCache instance that has a maximum size
     * of 1,000 objects and an expiry of 10 minutes. Override to adjust the settings or change the cache implementation.
     * In memory implementations of this cache will become troublesome if multiple processes are accessing the same
     * OCFL repository, in which case no cache or a distributed cache would be better choices.
     *
     * @param inventoryCache inventory cache
     * @return builder
     */
    public OcflRepositoryBuilder inventoryCache(Cache<String, Inventory> inventoryCache) {
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        return this;
    }

    /**
     * Changes the InventoryMapper to pretty print Inventory JSON files so that they are human readable but use more
     * disk space.
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
     */
    public OcflRepositoryBuilder contentPathConstraintProcessor(ContentPathConstraintProcessor contentPathConstraintProcessor) {
        this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
        return this;
    }

    /**
     * Set OCFL configuration options.
     *
     * @param config ocfl config
     * @return builder
     */
    public OcflRepositoryBuilder ocflConfig(OcflConfig config) {
        this.config = Enforce.notNull(config, "config cannot be null");
        return this;
    }

    /**
     * Used to specify the OCFL spec version. Default: 1.0
     *
     * @see #ocflConfig
     *
     * @param ocflVersion the OCFL version
     * @return builder
     */
    @Deprecated
    public OcflRepositoryBuilder ocflVersion(OcflVersion ocflVersion) {
        config.setOcflVersion(ocflVersion);
        return this;
    }

    /**
     * Used to specify the digest algorithm to use for newly created objects. Default: sha512.
     *
     * @see #ocflConfig
     *
     * @param digestAlgorithm digest algorithm
     * @return builder
     */
    @Deprecated
    public OcflRepositoryBuilder digestAlgorithm(DigestAlgorithm digestAlgorithm) {
        Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        config.setDefaultDigestAlgorithm(digestAlgorithm);
        return this;
    }

    /**
     * Used to specify the location of the content directory within newly created objects. Default: content.
     *
     * @see #ocflConfig
     *
     * @param contentDirectory content directory
     * @return builder
     */
    @Deprecated
    public OcflRepositoryBuilder contentDirectory(String contentDirectory) {
        Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
        config.setDefaultContentDirectory(contentDirectory);
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
     * @param storage the storage layer implementation that the OCFL repository should use
     * @param workDir the work directory to assemble versions in before they're moved to storage -- cannot be within the OCFL storage root
     * @return OcflRepository
     */
    public OcflRepository build(OcflStorage storage, Path workDir) {
        return buildDefault(storage, workDir);
    }

    /**
     * Constructs an OCFL repository that allows the use of the Mutable HEAD Extension. Brand new repositories are initialized.
     *
     * @param storage the storage layer implementation that the OCFL repository should use
     * @param workDir the work directory to assemble versions in before they're moved to storage -- cannot be within the OCFL storage root
     * @return MutableOcflRepository
     */
    public MutableOcflRepository buildMutable(OcflStorage storage, Path workDir) {
        return buildDefault(storage, workDir);
    }

    private DefaultOcflRepository buildDefault(OcflStorage storage, Path workDir) {
        Enforce.notNull(storage, "storage cannot be null");
        Enforce.notNull(workDir, "workDir cannot be null");

        storage.initializeStorage(config.getOcflVersion());

        Enforce.expressionTrue(Files.exists(workDir), workDir, "workDir must exist");
        Enforce.expressionTrue(Files.isDirectory(workDir), workDir, "workDir must be a directory");

        return new DefaultOcflRepository(storage, workDir,
                objectLock, inventoryCache, inventoryMapper,
                pathSanitizer, contentPathConstraintProcessor,
                config, digestThreadPoolSize, copyThreadPoolSize);
    }

}
