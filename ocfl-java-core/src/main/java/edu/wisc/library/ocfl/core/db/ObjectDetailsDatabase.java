package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.core.model.Inventory;

import java.nio.file.Path;

/**
 * Interface for interacting with OCFL object details that are stored in a database.
 */
public interface ObjectDetailsDatabase {

    /**
     * Retrieves ObjectDetails from the database. If no details can be found, null is returned.
     *
     * @param objectId the OCFL object id
     * @return ObjectDetails or null
     */
    OcflObjectDetails retrieveObjectDetails(String objectId);

    /**
     * Adds ObjectDetails to the database. In the case of a concurrent update, this operation will only fail if the inventory
     * digests are different.
     *
     * @param inventory the object's inventory
     * @param inventoryDigest the digest of the inventory
     * @param inventoryBytes the serialized inventory bytes
     */
    void addObjectDetails(Inventory inventory, String inventoryDigest, byte[] inventoryBytes);

    /**
     * Updates existing ObjectDetails in the database. The update is executed within a transaction. Before the transaction
     * is committed, the supplied runnable is executed. The transaction is only committed if the runnable completes without
     * exception.
     *
     * @param inventory the object's inventory
     * @param inventoryDigest the digest of the inventory
     * @param inventoryFile the path to the inventory on disk
     * @param runnable the code to execute within the update transaction
     */
    void updateObjectDetails(Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable);

    /**
     * Removes ObjectDetails from the database.
     *
     * @param objectId the OCFL object id
     * @param objectId the OCFL object id
     */
    void deleteObjectDetails(String objectId);

}
