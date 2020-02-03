package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.core.OcflVersion;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.storage.filesystem.FileSystemOcflStorage;

import java.nio.file.Path;
import java.util.Map;
import java.util.stream.Stream;

/**
 * Extension point that allows the OCFL repository to use any number of storage implementations so long as they
 * conform to this interface.
 *
 * <p>{@link #initializeStorage} MUST be called before the object may be used.
 *
 * @see FileSystemOcflStorage
 */
public interface OcflStorage {

    /**
     * Initializes the OCFL root. If it is an existing OCFL repository and the root has already been initialized, then
     * this method should do nothing. This method must be called before the object may be used.
     *
     * <p>layoutConfig may be null if the OCFL repository already exists, in which case the existing configuration is used.
     * If layoutConfig is specified for an existing repository, initialization will fail if the configurations do not match.
     *
     * @param ocflVersion the OCFL version
     * @param layoutConfig the storage layout configuration, may be null to auto-detect existing configuration
     * @param inventoryMapper the mapper to use for inventory serialization
     */
    void initializeStorage(OcflVersion ocflVersion, LayoutConfig layoutConfig, InventoryMapper inventoryMapper);

    /**
     * Returns a verified copy of the most recent object inventory. Null is returned if the object is not found.
     *
     * <p>DefaultOcflRepository calls this method within a read lock and caches the result.
     *
     * @param objectId the id of the object to load
     * @return the deserialized inventory or null if the object was not found
     * @throws FixityCheckException if the inventory fails its fixity check
     */
    Inventory loadInventory(String objectId);

    /**
     * Persists a fully composed object version. The contents of the stagingDir are the contents of the new version,
     * including a serialized copy of the updated inventory. It is safe to move the entire stagingDir to object-root/version.
     *
     * <p>DefaultOcflRepository calls this method from a write lock.
     *
     * <p>This method MUST ensure that the version to be persisted is not out of sync with the current object state, and
     * reject the request if it is.
     *
     * <p>After persisting the new version directory, the new inventory file is installed in the object root.
     *
     * @param inventory the updated object inventory
     * @param stagingDir the directory that contains the composed contents of the new object version
     * @throws ObjectOutOfSyncException if the version cannot be created because it already exists
     * @throws FixityCheckException if one of the files in the version fails its fixity check
     */
    void storeNewVersion(Inventory inventory, Path stagingDir);

    /**
     * Returns a map of {@code OcflFileRetriever} objects that are used to lazy-load object files. The map keys are the
     * object relative file paths of all of the files in the specified version of the object.
     *
     * @param inventory the object's inventory
     * @param versionId the id of the version to load
     * @return a map of {@code OcflFileRetriever} objects keyed off the object relative file paths of all of the files in the object
     */
    Map<String, OcflFileRetriever> getObjectStreams(Inventory inventory, VersionId versionId);

    /**
     * Reconstructs a complete object at the specified version in the stagingDir.
     *
     * <p>The fixity of every file must be checked after copying it to the stagingDir.
     *
     * @param inventory the deserialized object inventory
     * @param versionId the id of the version to reconstruct
     * @param stagingDir the location the reconstructed object should be assembled in
     * @throws FixityCheckException if one of the files fails its fixity check
     */
    void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir);

    /**
     * Permanently removes an object from the repository. Objects that have been purged are NOT recoverable. If an object
     * with the specified id cannot be found it is considered purged and no exception is thrown.
     *
     * <p>DefaultOcflRepository calls this method from a write lock.
     *
     * @param objectId the id of the object to purge
     */
    void purgeObject(String objectId);

    /**
     * Moves the mutable HEAD of any object into the object root and into an immutable version. The mutable HEAD does
     * not exist at the end of the operation.
     *
     * <p>DefaultOcflRepository calls this method from a write lock.
     *
     * @param oldInventory the deserialized inventory of the object BEFORE it was rewritten for the commit
     * @param newInventory the deserialized inventory of the object AFTER it was rewritten for the commit
     * @param stagingDir the path to the staging directory that contains the inventory files
     */
    void commitMutableHead(Inventory oldInventory, Inventory newInventory, Path stagingDir);

    /**
     * Permanently removes the mutable HEAD of an object. If the object does not have a mutable HEAD nothing happens.
     *
     * <p>DefaultOcflRepository calls this method from a write lock.
     *
     * @param objectId the id of the object to purge the mutable HEAD of
     */
    void purgeMutableHead(String objectId);

    /**
     * Returns true if an object with the specified id exists in the repository.
     *
     * @param objectId the id of the object
     * @return true if the object exists and false otherwise
     */
    boolean containsObject(String objectId);

    /**
     * Returns the path from the storage root to the object root.
     *
     * @param objectId the id of the object
     * @return the relative path from the storage root to the object root
     */
    String objectRootPath(String objectId);

    /**
     * Returns a stream of object ids for all of the OCFL objects stored in a repository. This stream is populated on demand,
     * and it may be quite slow. Remember to close the stream when you are done with it.
     *
     * @return stream of object ids
     */
    Stream<String> listObjectIds();

    /**
     * Shutsdown any resources the OclfStorage may have open, such as ExecutorServices. Once closed, additional requests
     * will be rejected. Calling this method is optional, and it is more efficient to just let the shutdown hooks take care
     * of closing the resources.
     */
    void close();

}
