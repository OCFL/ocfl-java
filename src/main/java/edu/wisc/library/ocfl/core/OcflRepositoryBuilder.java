package edu.wisc.library.ocfl.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.lock.InMemoryObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryType;
import edu.wisc.library.ocfl.core.storage.OcflStorage;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.TimeUnit;

/**
 * Constructs a local file system based OCFL repository sensible defaults that can be overriden prior to calling
 * build().
 *
 * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
 */
public class OcflRepositoryBuilder {

    private InventoryType inventoryType = OcflConstants.DEFAULT_INVENTORY_TYPE;
    private DigestAlgorithm digestAlgorithm = OcflConstants.DEFAULT_DIGEST_ALGORITHM;
    private String contentDirectory = OcflConstants.DEFAULT_CONTENT_DIRECTORY;
    private Set<DigestAlgorithm> fixityAlgorithms = new HashSet<>();

    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;

    /**
     * Constructs a local file system based OCFL repository sensible defaults that can be overriden prior to calling
     * build().
     *
     * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
     */
    public OcflRepositoryBuilder() {
        objectLock = new InMemoryObjectLock(10, TimeUnit.SECONDS);
        inventoryCache = new CaffeineCache<>(Caffeine.newBuilder()
                .expireAfterWrite(Duration.ofMinutes(10))
                .expireAfterAccess(Duration.ofMinutes(10))
                .maximumSize(1_000).build());
    }

    /**
     * Used to lock objects when read and writing. The default is an InMemoryObjectLock instance that will wait 10 seconds
     * for the lock before failing. Override to change the wait period or implement a different type of lock, such as
     * a distributed lock when coordinating across multiple instances accessing the same OCFL repository.
     *
     * @param objectLock
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
     * @param inventoryCache
     */
    public OcflRepositoryBuilder inventoryCache(Cache<String, Inventory> inventoryCache) {
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        return this;
    }

    /**
     * Used to specify the OCFL inventory type to apply to newly created inventories.
     *
     * @param inventoryType
     */
    public OcflRepositoryBuilder inventoryType(InventoryType inventoryType) {
        this.inventoryType = Enforce.notNull(inventoryType, "inventoryType cannot be null");
        return this;
    }

    /**
     * Used to specify the digest algorithm to use for newly created objects. Default: sha512.
     *
     * @param digestAlgorithm
     */
    public OcflRepositoryBuilder digestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        return this;
    }

    /**
     * Used to specify the location of the content directory within newly created objects. Default: content.
     *
     * @param contentDirectory
     */
    public OcflRepositoryBuilder contentDirectory(String contentDirectory) {
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
        return this;
    }

    /**
     * Used to specify the digest algorithms to use to compute additional fixity information. Default: none.
     *
     * @param fixityAlgorithms
     */
    public OcflRepositoryBuilder fixityAlgorithms(Set<DigestAlgorithm> fixityAlgorithms) {
        this.fixityAlgorithms = Enforce.notNull(fixityAlgorithms, "fixityAlgorithms cannot be null");
        return this;
    }

    /**
     * Constructs an OCFL repository. Brand new repositories are initialized.
     *
     * @param storage the storage layer implementation that the OCFL repository should use
     * @param workDir the work directory to assemble versions in before they're moved to storage
     */
    public OcflRepository build(OcflStorage storage, Path workDir) {
        Enforce.notNull(storage, "storage cannot be null");
        Enforce.notNull(workDir, "workDir cannot be null");

        storage.initializeStorage(OcflConstants.OCFL_VERSION);

        Enforce.expressionTrue(Files.exists(workDir), workDir, "workDir must exist");
        Enforce.expressionTrue(Files.isDirectory(workDir), workDir, "workDir must be a directory");

        return new DefaultOcflRepository(storage, workDir,
                objectLock, inventoryCache, fixityAlgorithms,
                inventoryType, digestAlgorithm, contentDirectory);
    }

}
