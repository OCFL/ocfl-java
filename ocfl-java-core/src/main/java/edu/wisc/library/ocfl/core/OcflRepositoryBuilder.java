/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core;

import com.github.benmanes.caffeine.cache.Caffeine;
import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.api.OcflRepository;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.cache.Cache;
import edu.wisc.library.ocfl.core.cache.CaffeineCache;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabase;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabaseBuilder;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.UnsupportedExtensionBehavior;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.FlatLayoutConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.lock.InMemoryObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.lock.ObjectLockBuilder;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.path.mapper.DirectLogicalPathMapper;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMapper;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers;
import edu.wisc.library.ocfl.core.storage.CachingOcflStorage;
import edu.wisc.library.ocfl.core.storage.ObjectDetailsDbOcflStorage;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.storage.OcflStorageBuilder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

/**
 * Constructs a local file system based OCFL repository sensible defaults that can be overridden prior to calling
 * {@link #build()}.
 *
 * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
 */
public class OcflRepositoryBuilder {

    private OcflStorage storage;
    private OcflConfig config;
    private OcflExtensionConfig defaultLayoutConfig;
    private Path workDir;
    private boolean verifyStaging;

    private ObjectLock objectLock;
    private Cache<String, Inventory> inventoryCache;
    private InventoryMapper inventoryMapper;
    private LogicalPathMapper logicalPathMapper;
    private ContentPathConstraintProcessor contentPathConstraintProcessor;
    private ObjectDetailsDatabase objectDetailsDb;
    private UnsupportedExtensionBehavior unsupportedBehavior;
    private Set<String> ignoreUnsupportedExtensions;

    /**
     * Constructs a local file system based OCFL repository sensible defaults that can be overridden prior to calling
     * {@link #build()}.
     *
     * <p>Important: The same OcflRepositoryBuilder instance MUST NOT be used to initialize multiple repositories.
     */
    public OcflRepositoryBuilder() {
        config = new OcflConfig();
        objectLock = new InMemoryObjectLock(10, TimeUnit.SECONDS);
        inventoryCache = new CaffeineCache<>(Caffeine.newBuilder()
                .expireAfterAccess(Duration.ofMinutes(10))
                .maximumSize(512)
                .build());
        inventoryMapper = InventoryMapper.defaultMapper();
        logicalPathMapper = LogicalPathMappers.directMapper();
        contentPathConstraintProcessor = ContentPathConstraints.minimal();
        unsupportedBehavior = UnsupportedExtensionBehavior.FAIL;
        ignoreUnsupportedExtensions = Collections.emptySet();
        verifyStaging = true;
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
     * Configure the storage layer the repository should use. Required.
     *
     * @param configureStorage storage configurer
     * @return builder
     */
    public OcflRepositoryBuilder storage(Consumer<OcflStorageBuilder> configureStorage) {
        var builder = OcflStorageBuilder.builder();
        configureStorage.accept(builder);
        this.storage = builder.build();
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
     * Configures the object lock that's used. The default is an {@link InMemoryObjectLock} instance that will wait 10 seconds
     * for the lock before failing. Set the DataSource on the builder if you'd like to use a DB lock instead.
     *
     * @param configureLock use to configure the lock
     * @return builder
     * @see ObjectLockBuilder
     */
    public OcflRepositoryBuilder objectLock(Consumer<ObjectLockBuilder> configureLock) {
        var builder = new ObjectLockBuilder();
        configureLock.accept(builder);
        this.objectLock = builder.build();
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
     * Used to store details about OCFL objects in the repository. This is primarily intended to be used when working
     * with a cloud object store like S3.
     *
     * @param configureDb use to configure the object details db
     * @return builder
     * @see ObjectDetailsDatabaseBuilder
     */
    public OcflRepositoryBuilder objectDetailsDb(Consumer<ObjectDetailsDatabaseBuilder> configureDb) {
        var builder = new ObjectDetailsDatabaseBuilder();
        configureDb.accept(builder);
        this.objectDetailsDb = builder.build();
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
     * Overrides the default {@link DirectLogicalPathMapper}. {@link LogicalPathMapper}s are used to map logical paths
     * to content paths so that they can safely be written to disk. The default behaviour is to map logical paths to
     * content paths directly without any changes.
     *
     * @see edu.wisc.library.ocfl.core.path.mapper.LogicalPathMappers
     *
     * @param logicalPathMapper logical path mapper
     * @return builder
     */
    public OcflRepositoryBuilder logicalPathMapper(LogicalPathMapper logicalPathMapper) {
        this.logicalPathMapper = Enforce.notNull(logicalPathMapper, "logicalPathMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default ContentPathConstraintProcessor that is used to enforce restrictions on what constitutes a valid
     * content path. By default, there are no restrictions.
     *
     * <p>Path constraints are applied after logical paths are mapped to content paths, and are used to attempt to
     * ensure the portability of content paths. The following default generic constraint configurations are provided:
     *
     * <ul>
     *     <li>{@link ContentPathConstraints#unix()}</li>
     *     <li>{@link ContentPathConstraints#windows()}</li>
     *     <li>{@link ContentPathConstraints#cloud()}</li>
     *     <li>{@link ContentPathConstraints#all()}</li>
     *     <li>{@link ContentPathConstraints#minimal()}</li>
     * </ul>
     *
     * <p>Constraints should be applied that target filesystems that are NOT the local filesystem. The local filesystem
     * will enforce its own constraints just fine. This mechanism is intended to enforce path constraints that the local
     * filesystem does not.
     *
     * <p>If you use the builtin constraint processor, the following constraints are ALWAYS applied:
     *
     * <ul>
     *     <li>Cannot have a leading OR trailing /</li>
     *     <li>Cannot contain the following filenames: '.', '..'</li>
     *     <li>Cannot contain an empty filename</li>
     *     <li>Windows only: Cannot contain a \</li>
     * </ul>
     *
     * @param contentPathConstraints constraint processor
     * @return builder
     * @see ContentPathConstraints
     */
    public OcflRepositoryBuilder contentPathConstraints(ContentPathConstraintProcessor contentPathConstraints) {
        this.contentPathConstraintProcessor =
                Enforce.notNull(contentPathConstraints, "contentPathConstraints cannot be null");
        return this;
    }

    /**
     * Sets OCFL configuration options.
     *
     * @param config OCFL config
     * @return builder
     */
    public OcflRepositoryBuilder ocflConfig(OcflConfig config) {
        this.config = Enforce.notNull(config, "config cannot be null");
        return this;
    }

    /**
     * Sets OCFL configuration options.
     *
     * @param configureConfig configures the OCFL config
     * @return builder
     */
    public OcflRepositoryBuilder ocflConfig(Consumer<OcflConfig> configureConfig) {
        this.config = new OcflConfig();
        configureConfig.accept(this.config);
        return this;
    }

    /**
     * Sets the default OCFL storage layout configuration. A layout MUST be specified if the OCFL repository does not
     * yet exist. If the repository does exist and it has a storage layout defined, then a layout does not need to
     * be specified and, if it is specified here, it will be ignored.
     *
     * @see HashedNTupleLayoutConfig
     * @see HashedNTupleIdEncapsulationLayoutConfig
     * @see FlatLayoutConfig
     *
     * @param defaultLayoutConfig the default storage layout configuration
     * @return builder
     */
    public OcflRepositoryBuilder defaultLayoutConfig(OcflExtensionConfig defaultLayoutConfig) {
        this.defaultLayoutConfig = Enforce.notNull(defaultLayoutConfig, "defaultLayoutConfig cannot be null");
        return this;
    }

    /**
     * Set the behavior when an unsupported extension is encountered. By default, ocfl-java will not operate on
     * repositories or objects that contain unsupported extensions. Set this value to WARN, if you'd like ocfl-java
     * to log a WARNing, but continue to operate instead.
     *
     * <p>Specific unsupported extensions may be ignored individually using {@code ignoreUnsupportedExtensions}
     *
     * @param unsupportedBehavior FAIL to throw an exception or WARN to log a warning
     * @return builder
     */
    public OcflRepositoryBuilder unsupportedExtensionBehavior(UnsupportedExtensionBehavior unsupportedBehavior) {
        this.unsupportedBehavior = Enforce.notNull(unsupportedBehavior, "unsupportedExtensionBehavior cannot be null");
        return this;
    }

    /**
     * Sets a list of unsupported extensions that should be ignored. If the unsupported extension behavior
     * is set to FAIL, this means that these extensions will produce log WARNings if they are encountered. If
     * the behavior is set to WARN, then these extensions will be silently ignored.
     *
     * @param ignoreUnsupportedExtensions set of unsupported extension names that should be ignored
     * @return builder
     */
    public OcflRepositoryBuilder ignoreUnsupportedExtensions(Set<String> ignoreUnsupportedExtensions) {
        this.ignoreUnsupportedExtensions =
                Enforce.notNull(ignoreUnsupportedExtensions, "ignoreUnsupportedExtensions cannot be null");
        return this;
    }

    /**
     * Configures whether to verify the contents of a staged version before it is moved into the OCFL object. This
     * verification includes iterating over all of the files in the version and ensuring they match the expectations
     * in the inventory.
     *
     * <p>This verification is enabled by default out of conservatism. It is unlikely that there will ever be a problem
     * for it to uncover, and it can be safely disabled if there are concerns about performance on slower filesystems.
     *
     * @param verifyStaging true if the contents of a stage version should be double-checked
     * @return builder
     */
    public OcflRepositoryBuilder verifyStaging(boolean verifyStaging) {
        this.verifyStaging = verifyStaging;
        return this;
    }

    /**
     * Constructs an OCFL repository. Brand new repositories are initialized.
     * <p>
     * Remember to call {@link OcflRepository#close()} when you are done with the repository.
     *
     * @return OcflRepository
     */
    public OcflRepository build() {
        return buildInternal(DefaultOcflRepository.class);
    }

    /**
     * Constructs an OCFL repository that allows the use of the Mutable HEAD Extension. Brand new repositories are initialized.
     * <p>
     * Remember to call {@link OcflRepository#close()} when you are done with the repository.
     *
     * @return MutableOcflRepository
     */
    public MutableOcflRepository buildMutable() {
        return buildInternal(DefaultMutableOcflRepository.class);
    }

    private <T extends OcflRepository> T buildInternal(Class<T> clazz) {
        Enforce.notNull(storage, "storage cannot be null");
        Enforce.notNull(workDir, "workDir cannot be null");

        var supportEvaluator = new ExtensionSupportEvaluator(unsupportedBehavior, ignoreUnsupportedExtensions);

        var wrappedStorage = cache(db(storage));
        var initResult = wrappedStorage.initializeStorage(
                config.getOcflVersion(), defaultLayoutConfig, inventoryMapper, supportEvaluator);

        // Default the OCFL version to whatever was in the storage root
        if (config.getOcflVersion() == null) {
            config.setOcflVersion(initResult.getOcflVersion());
        }

        Enforce.expressionTrue(Files.exists(workDir), workDir, "workDir must exist");
        Enforce.expressionTrue(Files.isDirectory(workDir), workDir, "workDir must be a directory");

        if (MutableOcflRepository.class.isAssignableFrom(clazz)) {
            return clazz.cast(new DefaultMutableOcflRepository(
                    wrappedStorage,
                    workDir,
                    objectLock,
                    inventoryMapper,
                    logicalPathMapper,
                    contentPathConstraintProcessor,
                    config,
                    verifyStaging));
        }

        return clazz.cast(new DefaultOcflRepository(
                wrappedStorage,
                workDir,
                objectLock,
                inventoryMapper,
                logicalPathMapper,
                contentPathConstraintProcessor,
                config,
                verifyStaging));
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
