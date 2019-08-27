package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.VersionId;

import java.nio.file.Path;

public interface OcflStorage {

    /**
     * Returns a verified copy of the most recent object inventory.
     *
     * DefaultOcflRepository calls this method within a read lock, and caches the result.
     *
     * @param objectId
     * @return
     */
    Inventory loadInventory(String objectId);

    /**
     * Persists a fully composed object version. The contents of the stagingDir should be the contents of the new
     * version. It is safe to move the entire stagingDir to object-root/version.
     *
     * DefaultOcflRepository calls this method from a write lock.
     *
     * This method must ensure that the version to be persisted is not out of sync with the current object state, and
     * reject the request if it is.
     *
     * After persisting the new version directory, the new inventory file is installed in the object root.
     *
     * @param inventory
     * @param stagingDir
     */
    void storeNewVersion(Inventory inventory, Path stagingDir);

    /**
     * Reconstructs a complete object at the specified version in the stagingDir.
     *
     * The fixity of every file must be checked after copying it to the stagingDir.
     *
     * @param inventory
     * @param versionId
     * @param stagingDir
     */
    void reconstructObjectVersion(Inventory inventory, VersionId versionId, Path stagingDir);

    /**
     * Write the file that corresponds to the specified fileId (digest) to the destinationPath.
     *
     * The fixity of the file must be checked after copying it to the destinationPath.
     *
     * @param inventory
     * @param fileId
     * @param destinationPath
     * @param ocflOptions
     */
    void retrieveFile(Inventory inventory, String fileId, Path destinationPath, OcflOption... ocflOptions);

}
