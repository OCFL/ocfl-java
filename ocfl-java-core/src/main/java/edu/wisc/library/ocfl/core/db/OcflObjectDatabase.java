package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class OcflObjectDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(OcflObjectDatabase.class);

    private DataSource dataSource;
    private boolean storeInventory;

    public OcflObjectDatabase(DataSource dataSource, boolean storeInventory) {
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        this.storeInventory = storeInventory;
    }

    public OcflObjectDetails retrieveObjectDetails(String objectId) {
        OcflObjectDetails details = null;

        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("SELECT" +
                    " object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp" +
                    " FROM ocfl_object WHERE object_id = ?")) {
                statement.setString(1, objectId);

                try (var rs = statement.executeQuery()) {
                    if (rs.next()) {
                        details = new OcflObjectDetails()
                                .setObjectId(rs.getString(1))
                                .setVersionId(VersionId.fromString(rs.getString(2)))
                                .setObjectRootPath(rs.getString(3))
                                .setRevisionId(revisionIdFromString(rs.getString(4)))
                                .setInventoryDigest(rs.getString(5))
                                .setDigestAlgorithm(DigestAlgorithm.fromOcflName(rs.getString(6)))
                                .setInventory(rs.getBytes(7))
                                .setUpdateTimestamp(rs.getTimestamp(8).toLocalDateTime());
                    }
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }

        return details;
    }

    public void addObject(Inventory inventory, String inventoryDigest, Path inventoryFile) {
        try {
            updateObject(inventory, inventoryDigest, inventoryFile, () -> {});
        } catch (ObjectOutOfSyncException e) {
            var details = retrieveObjectDetails(inventory.getId());
            if (details != null && inventoryDigest.equalsIgnoreCase(details.getInventoryDigest())) {
                // everything's fine
                return;
            }
            throw e;
        }
    }

    public void deleteObject(String objectId) {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("DELETE FROM ocfl_object WHERE object_id = ?")) {
                statement.setString(1, objectId);
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    public void updateObject(Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                if (storeInventory) {
                    insertWithInventory(connection, inventory, inventoryDigest, inventoryFile);
                } else {
                    insertWithoutInventory(connection, inventory, inventoryDigest);
                }

                runnable.run();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                safeEnableAutoCommit(connection);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void insertWithInventory(Connection connection, Inventory inventory, String inventoryDigest, Path inventoryFile) throws SQLException {
        try (var updateStatement = connection.prepareStatement("INSERT INTO ocfl_object" +
                " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (object_id) DO UPDATE SET" +
                " (version_id, revision_id, inventory_digest, inventory, update_timestamp) =" +
                " (EXCLUDED.version_id, EXCLUDED.revision_id, EXCLUDED.inventory_digest, EXCLUDED.inventory, EXCLUDED.update_timestamp)" +
                " WHERE ocfl_object.version_id = ? AND ocfl_object.revision_id = ?");
             var inventoryStream = Files.newInputStream(inventoryFile)) {

            updateStatement.setString(1, inventory.getId());
            updateStatement.setString(2, inventory.getHead().toString());
            updateStatement.setString(3, inventory.getObjectRootPath());
            updateStatement.setString(4, revisionIdStr(inventory.getRevisionId()));
            updateStatement.setString(5, inventoryDigest);
            updateStatement.setString(6, inventory.getDigestAlgorithm().getOcflName());
            updateStatement.setBinaryStream(7, inventoryStream, Files.size(inventoryFile));
            updateStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setString(9, previousVersionIdStr(inventory.getHead(), inventory.getRevisionId()));
            updateStatement.setString(10, previousRevisionIdStr(inventory.getRevisionId()));

            verifyUpdate(updateStatement.executeUpdate(), inventory.getId());
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    private void insertWithoutInventory(Connection connection, Inventory inventory, String inventoryDigest) throws SQLException {
        try (var updateStatement = connection.prepareStatement("INSERT INTO ocfl_object" +
                " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, update_timestamp)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (object_id) DO UPDATE SET" +
                " (version_id, revision_id, inventory_digest, inventory, update_timestamp) =" +
                " (EXCLUDED.version_id, EXCLUDED.revision_id, EXCLUDED.inventory_digest, EXCLUDED.inventory, EXCLUDED.update_timestamp)" +
                " WHERE ocfl_object.version_id = ? AND ocfl_object.revision_id = ?")) {

            updateStatement.setString(1, inventory.getId());
            updateStatement.setString(2, inventory.getHead().toString());
            updateStatement.setString(3, inventory.getObjectRootPath());
            updateStatement.setString(4, revisionIdStr(inventory.getRevisionId()));
            updateStatement.setString(5, inventoryDigest);
            updateStatement.setString(6, inventory.getDigestAlgorithm().getOcflName());
            updateStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setString(8, previousVersionIdStr(inventory.getHead(), inventory.getRevisionId()));
            updateStatement.setString(9, previousRevisionIdStr(inventory.getRevisionId()));

            verifyUpdate(updateStatement.executeUpdate(), inventory.getId());
        }
    }

    private void verifyUpdate(int updateResult, String objectId) {
        if (updateResult == 0) {
            throw new ObjectOutOfSyncException(String.format(
                    "Cannot update object %s because its state is out of sync with the current state in the database.", objectId));
        }
    }

    private String revisionIdStr(RevisionId revisionId) {
        return revisionId == null ? null : revisionId.toString();
    }

    private RevisionId revisionIdFromString(String revisionId) {
        if (revisionId == null) {
            return null;
        }
        return RevisionId.fromString(revisionId);
    }

    private String previousVersionIdStr(VersionId versionId, RevisionId revisionId) {
        if (revisionId == null || RevisionId.R1.equals(revisionId)) {
            if (VersionId.V1.equals(versionId)) {
                return null;
            }
            return versionId.previousVersionId().toString();
        }
        return versionId.toString();
    }

    private String previousRevisionIdStr(RevisionId currentRevisionId) {
        if (currentRevisionId == null || RevisionId.R1.equals(currentRevisionId)) {
            return null;
        }
        return currentRevisionId.previousRevisionId().toString();
    }

    // TODO move to calling code
//    private Inventory parseInventory(byte[] inventoryBytes, OcflObjectDetails details) {
//        var actualDigest = DigestUtil.computeDigestHex(details.getDigestAlgorithm(), inventoryBytes);
//
//        if (!details.getInventoryDigest().equalsIgnoreCase(actualDigest)) {
//            throw new FixityCheckException(String.format("Expected %s digest: %s; Actual: %s",
//                    details.getDigestAlgorithm(), details.getInventoryDigest(), actualDigest));
//        }
//
//        if (details.getRevisionId() == null) {
//            return inventoryMapper.read(details.getObjectRootPath(), new ByteArrayInputStream(inventoryBytes));
//        } else {
//            return inventoryMapper.readMutableHead(details.getObjectRootPath(), details.getRevisionId(), new ByteArrayInputStream(inventoryBytes));
//        }
//    }

    private void safeEnableAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (Exception e) {
            LOG.warn("Failed to enable autocommit", e);
        }
    }

}
