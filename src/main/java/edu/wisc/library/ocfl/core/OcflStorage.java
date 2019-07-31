package edu.wisc.library.ocfl.core;

import edu.wisc.library.ocfl.core.model.Inventory;

import java.nio.file.Path;
import java.util.Map;
import java.util.Set;

public interface OcflStorage {

    Inventory loadInventory(String objectId);

    void storeNewVersion(Inventory inventory, Path stagingDir);

    void reconstructObjectVersion(Inventory inventory, Map<String, Set<String>> fileMap, Path stagingDir);

}
