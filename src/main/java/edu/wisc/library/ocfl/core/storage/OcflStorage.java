package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.FixityCheckException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.VersionId;

import java.nio.file.Path;

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
     * Write the file that corresponds to the specified fileId (digest) to the destinationPath.
     *
     * <p>The fixity of the file must be checked after copying it to the destinationPath.
     *
     * @param inventory the deserialized object inventory
     * @param fileId the digest of the file to retrieve
     * @param destinationPath the path to write the file to
     * @param ocflOptions Optional. Use {@code OcflOption.OVERWRITE} to overwrite an existing
     *                    file at the destinationPath
     * @throws FixityCheckException if the file fails its fixity check
     */
    void retrieveFile(Inventory inventory, String fileId, Path destinationPath, OcflOption... ocflOptions);

    /**
     * Initializes the OCFL root. If it is an existing OCFL repository and the root has already been initialized, then
     * this method should do nothing.
     *
     * @param ocflVersion the OCFL version string, such as "ocfl_1.0"
     */
    void initializeStorage(String ocflVersion);

}
