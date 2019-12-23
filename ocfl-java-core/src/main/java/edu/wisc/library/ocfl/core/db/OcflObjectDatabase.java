package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.core.model.Inventory;

import java.nio.file.Path;

public interface OcflObjectDatabase {

    OcflObjectDetails retrieveObjectDetails(String objectId);

    void addObjectDetails(Inventory inventory, String inventoryDigest, byte[] inventoryBytes);

    void updateObjectDetails(Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable);

    void deleteObjectDetails(String objectId);

}
