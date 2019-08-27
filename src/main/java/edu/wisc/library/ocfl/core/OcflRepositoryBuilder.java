package edu.wisc.library.ocfl.core;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.lock.InMemoryObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.mapping.CachingObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.ObjectIdPathMapper;
import edu.wisc.library.ocfl.core.mapping.PairTreeEncoder;
import edu.wisc.library.ocfl.core.mapping.PairTreeObjectIdPathMapper;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.InventoryType;
import edu.wisc.library.ocfl.core.storage.FileSystemOcflStorage;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.NamasteFileWriter;

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

    private OcflStorage storage;
    private ObjectIdPathMapper objectIdPathMapper;
    private NamasteFileWriter namasteFileWriter;
    private ObjectMapper objectMapper;
    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private Path workDir;

    /**
     * Constructs a local file system based OCFL repository sensible defaults that can be overriden prior to calling
     * build().
     *
     * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
     */
    public OcflRepositoryBuilder() {
        objectIdPathMapper = new CachingObjectIdPathMapper(
                new PairTreeObjectIdPathMapper(
                        new PairTreeEncoder(false), "obj", 4),
                new CaffeineCache<>(Caffeine.newBuilder().expireAfterAccess(Duration.ofMinutes(10)).build()));
        namasteFileWriter = new NamasteFileWriter();
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
        objectLock = new InMemoryObjectLock(10, TimeUnit.SECONDS);
        inventoryCache = new CaffeineCache<>(Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(10))
                .expireAfterWrite(Duration.ofMinutes(10))
                .maximumSize(1_000).build());
    }

    /**
     * Used to set the OCFL storage layer. By default it uses FileSystemOcflStorage.
     *
     * @param storage
     * @return
     */
    public OcflRepositoryBuilder storage(OcflStorage storage) {
        this.storage = Enforce.notNull(storage, "storage cannot be null");
        return this;
    }

    /**
     * Used to map object ids to paths under the repository root. The default implementation is a cached PairTreeObjectIdPathMapper,
     * using pairtree cleaning, a 4 character encapsulation string, and "obj" as the default encapsulation directory name.
     *
     * @param objectIdPathMapper
     * @return
     */
    public OcflRepositoryBuilder objectIdPathMapper(ObjectIdPathMapper objectIdPathMapper) {
        this.objectIdPathMapper = Enforce.notNull(objectIdPathMapper, "objectIdPathMapper cannot be null");
        return this;
    }

    /**
     * Used to write namaste files.
     *
     * @param namasteFileWriter
     * @return
     */
    public OcflRepositoryBuilder namasteFileWriter(NamasteFileWriter namasteFileWriter) {
        this.namasteFileWriter = Enforce.notNull(namasteFileWriter, "namasteFileWriter cannot be null");
        return this;
    }

    /**
     * Used to serialize and deserialize inventories. It is not recommended to override this.
     *
     * @param objectMapper
     * @return
     */
    public OcflRepositoryBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Used to lock objects when read and writing. The default is an InMemoryObjectLock instance that will wait 10 seconds
     * for the lock before failing. Override to change the wait period or implement a different type of lock, such as
     * a distributed lock when coordinating across multiple instances accessing the same OCFL repository.
     *
     * @param objectLock
     * @return
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
     * @return
     */
    public OcflRepositoryBuilder inventoryCache(Cache<String, Inventory> inventoryCache) {
        this.inventoryCache = Enforce.notNull(inventoryCache, "inventoryCache cannot be null");
        return this;
    }

    /**
     * Used to specify the OCFL inventory type to apply to newly created inventories.
     *
     * @param inventoryType
     * @return
     */
    public OcflRepositoryBuilder inventoryType(InventoryType inventoryType) {
        this.inventoryType = Enforce.notNull(inventoryType, "inventoryType cannot be null");
        return this;
    }

    /**
     * Used to specify the digest algorithm to use for newly created objects. Default: sha512.
     *
     * @param digestAlgorithm
     * @return
     */
    public OcflRepositoryBuilder digestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = Enforce.notNull(digestAlgorithm, "digestAlgorithm cannot be null");
        return this;
    }

    /**
     * Used to specify the location of the content directory within newly created objects. Default: content.
     *
     * @param contentDirectory
     * @return
     */
    public OcflRepositoryBuilder contentDirectory(String contentDirectory) {
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
        return this;
    }

    /**
     * Used to specify the digest algorithms to use to compute additional fixity information. Default: none.
     *
     * @param fixityAlgorithms
     * @return
     */
    public OcflRepositoryBuilder fixityAlgorithms(Set<DigestAlgorithm> fixityAlgorithms) {
        this.fixityAlgorithms = Enforce.notNull(fixityAlgorithms, "fixityAlgorithms cannot be null");
        return this;
    }

    /**
     * Used to specify the directory that is used as work space to assemble new object versions. The default location
     * is the deposit directory within the repository root.
     *
     * @param workDir
     */
    public OcflRepositoryBuilder workDir(Path workDir) {
        this.workDir = Enforce.notNull(workDir, "workDir cannot be null");
        Enforce.expressionTrue(Files.exists(workDir), workDir, "workDir must exist");
        Enforce.expressionTrue(Files.isDirectory(workDir), workDir, "workDir must be a directory");
        return this;
    }

    /**
     * Constructs an OCFL repository. Brand new repositories are initialized on disk.
     *
     * @param repositoryRoot The path to the root directory of the OCFL repository
     * @return
     */
    public OcflRepository build(Path repositoryRoot) {
        Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");

        initializeRepo(repositoryRoot);

        if (storage == null) {
            storage = new FileSystemOcflStorage(repositoryRoot, objectIdPathMapper, objectMapper, namasteFileWriter);
        }

        if (workDir == null) {
            workDir = repositoryRoot.resolve(OcflConstants.DEPOSIT_DIRECTORY);
            FileUtil.createDirectories(workDir);
        }

        return new DefaultOcflRepository(storage, objectMapper, workDir,
                objectLock, inventoryCache, fixityAlgorithms,
                inventoryType, digestAlgorithm, contentDirectory);
    }

    private void initializeRepo(Path repositoryRoot) {
        // TODO perhaps repository initialization should be moved
        if (!Files.exists(repositoryRoot)) {
            FileUtil.createDirectories(repositoryRoot);
        } else {
            Enforce.expressionTrue(Files.isDirectory(repositoryRoot), repositoryRoot,
                    "repositoryRoot must be a directory");
        }

        if (!Files.exists(repositoryRoot.resolve(namasteFileWriter.namasteFileName(OcflConstants.OCFL_VERSION)))) {
            namasteFileWriter.writeFile(repositoryRoot, OcflConstants.OCFL_VERSION);
        }

        // TODO add copy of OCFL spec -- ocfl_1.0.txt
        // TODO add storage layout description -- ocfl_layout.json

        // TODO verify can read OCFL version
        // TODO how to verify that the repo is configured correctly to read the layout of an existing structure?
    }

}
