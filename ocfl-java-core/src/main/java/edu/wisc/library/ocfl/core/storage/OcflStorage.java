package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflFileRetriever;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.VersionId;

import java.io.InputStream;
import java.nio.file.Path;
import java.util.Map;

/**
 * Extension point that allows the OCFL repository to use any number of storage implementations so long as they
 * conform to this interface.
 *
 * @see FileSystemOcflStorage
 */
public interface OcflStorage {

    /**
     * Returns a verified copy of the most recent object inventory. Null is returned if the object is not found.
     *
     * <p>DefaultOcflRepository calls this method within a read lock and caches the result.
     *
     * @param objectId
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
    Map<String, OcflFileRetriever> lazyLoadObject(Inventory inventory, VersionId versionId);

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
     * Write the file that corresponds to the specified fileId (digest) to an InputStream.
     *
     * @param inventory the deserialized object inventory
     * @param fileId the digest of the file to retrieve
     */
    InputStream retrieveFile(Inventory inventory, String fileId);

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
     * Initializes the OCFL root. If it is an existing OCFL repository and the root has already been initialized, then
     * this method should do nothing.
     *
     * @param ocflVersion the OCFL version string, such as "ocfl_1.0"
     */
    void initializeStorage(String ocflVersion);

}
