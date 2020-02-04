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

import edu.wisc.library.ocfl.api.MutableOcflRepository;
import edu.wisc.library.ocfl.api.OcflObjectUpdater;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.inventory.MutableHeadInventoryCommitter;
import edu.wisc.library.ocfl.core.lock.ObjectLock;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.storage.OcflStorage;
import edu.wisc.library.ocfl.core.util.FileUtil;
import edu.wisc.library.ocfl.core.util.UncheckedFiles;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Path;
import java.util.function.Consumer;

/**
 * Extends the OCFL repository to support OCFL objects with mutable HEADs. This is not supported in the official spec.
 *
 * @see OcflRepositoryBuilder
 */
// TODO This type of hierarchy is not sustainable if there are more types of OCFL extensions. Refactor when other extensions exist
public class DefaultMutableOcflRepository extends DefaultOcflRepository implements MutableOcflRepository {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultMutableOcflRepository.class);

    /**
     * @see OcflRepositoryBuilder
     *
     * @param storage storage layer
     * @param workDir path to the directory to use for assembling ocfl versions
     * @param objectLock locking client
     * @param inventoryMapper object mapper for serializing inventories
     * @param pathSanitizer content path sanitizer
     * @param contentPathConstraintProcessor content path constraint processor
     * @param config ocfl defaults configuration
     * @param digestThreadPoolSize number of threads to use for computing digests
     * @param copyThreadPoolSize number of threads to use for copying files
     */
    public DefaultMutableOcflRepository(OcflStorage storage, Path workDir,
                                        ObjectLock objectLock,
                                        InventoryMapper inventoryMapper,
                                        PathSanitizer pathSanitizer,
                                        ContentPathConstraintProcessor contentPathConstraintProcessor,
                                        OcflConfig config,
                                        int digestThreadPoolSize, int copyThreadPoolSize) {
        super(storage, workDir, objectLock, inventoryMapper,
                pathSanitizer, contentPathConstraintProcessor, config,
                digestThreadPoolSize, copyThreadPoolSize);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId stageChanges(ObjectVersionId objectVersionId, CommitInfo commitInfo, Consumer<OcflObjectUpdater> objectUpdater) {
        ensureOpen();

        Enforce.notNull(objectVersionId, "objectVersionId cannot be null");
        Enforce.notNull(objectUpdater, "objectUpdater cannot be null");

        var inventory = loadInventory(objectVersionId);

        if (inventory == null) {
            // Note: If the mutable HEAD creation fails, the object with the empty version remains
            inventory = createAndPersistEmptyVersion(objectVersionId);
        }

        enforceObjectVersionForUpdate(objectVersionId, inventory);

        var stagingDir = createStagingDir(objectVersionId);
        var contentDir = UncheckedFiles.createDirectories(resolveRevisionDir(inventory, stagingDir)).getParent();

        var inventoryUpdater = inventoryUpdaterBuilder.buildCopyStateMutable(inventory);
        var addFileProcessor = addFileProcessorBuilder.build(inventoryUpdater, contentDir, inventory.getDigestAlgorithm());
        var updater = new DefaultOcflObjectUpdater(inventory, inventoryUpdater, contentDir, addFileProcessor);

        try {
            objectUpdater.accept(updater);
            var newInventory = buildNewInventory(inventoryUpdater, commitInfo);
            writeNewVersion(newInventory, stagingDir);
            return ObjectVersionId.version(objectVersionId.getObjectId(), newInventory.getHead());
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public ObjectVersionId commitStagedChanges(String objectId, CommitInfo commitInfo) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        var inventory = requireInventory(ObjectVersionId.head(objectId));

        if (inventory.hasMutableHead()) {
            var newInventory = MutableHeadInventoryCommitter.commit(inventory, now(), commitInfo);
            var stagingDir = FileUtil.createTempDir(workDir, objectId);
            writeInventory(newInventory, stagingDir);

            try {
                objectLock.doInWriteLock(inventory.getId(), () -> storage.commitMutableHead(inventory, newInventory, stagingDir));
            } finally {
                FileUtil.safeDeletePath(stagingDir);
            }
        }

        return ObjectVersionId.version(objectId, inventory.getHead());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void purgeStagedChanges(String objectId) {
        ensureOpen();

        Enforce.notBlank(objectId, "objectId cannot be blank");

        objectLock.doInWriteLock(objectId, () -> storage.purgeMutableHead(objectId));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean hasStagedChanges(String objectId) {
        ensureOpen();
        // TODO return false if object does not exist?
        Enforce.notBlank(objectId, "objectId cannot be blank");
        var inventory = requireInventory(ObjectVersionId.head(objectId));
        return inventory.hasMutableHead();
    }

    private Inventory createAndPersistEmptyVersion(ObjectVersionId objectId) {
        LOG.info("Creating object {} with an empty version.", objectId.getObjectId());

        var stubInventory = createStubInventory(objectId);
        var stagingDir = FileUtil.createTempDir(workDir, objectId.getObjectId());
        UncheckedFiles.createDirectories(resolveContentDir(stubInventory, stagingDir));

        try {
            var inventoryBuilder = Inventory.builder(stubInventory);
            var inventory = inventoryBuilder
                    .addHeadVersion(Version.builder()
                            .commitInfo(new CommitInfo()
                                    .setMessage("Auto-generated empty object version.")
                                    .setUser(new User().setName("ocfl-java")))
                            .created(now())
                            .build())
                    .build();

            writeNewVersion(inventory, stagingDir);
            return inventory;
        } finally {
            FileUtil.safeDeletePath(stagingDir);
        }
    }

    private Path resolveRevisionDir(Inventory inventory, Path parent) {
        var contentDir = resolveContentDir(inventory, parent);
        var newRevision = inventory.nextRevisionId();
        return contentDir.resolve(newRevision.toString());
    }

}
