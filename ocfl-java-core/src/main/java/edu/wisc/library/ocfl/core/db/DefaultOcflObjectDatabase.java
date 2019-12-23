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
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;

public class DefaultOcflObjectDatabase implements OcflObjectDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(DefaultOcflObjectDatabase.class);

    private DataSource dataSource;
    private boolean storeInventory;

    public DefaultOcflObjectDatabase(DataSource dataSource, boolean storeInventory) {
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        this.storeInventory = storeInventory;
    }

    @Override
    public OcflObjectDetails retrieveObjectDetails(String objectId) {
        OcflObjectDetails details = null;

        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("SELECT" +
                    " object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp" +
                    " FROM ocfl_object_details WHERE object_id = ?")) {
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

    @Override
    public void addObjectDetails(Inventory inventory, String inventoryDigest, byte[] inventoryBytes) {
        try {
            updateObjectDetailsInternal(inventory, inventoryDigest, new ByteArrayInputStream(inventoryBytes), () -> {});
        } catch (ObjectOutOfSyncException e) {
            var digest = retrieveDigest(inventory.getId());
            if (inventoryDigest.equalsIgnoreCase(digest)) {
                // everything's fine
                return;
            }
            throw e;
        }
    }

    @Override
    public void updateObjectDetails(Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable) {
        try (var inventoryStream = Files.newInputStream(inventoryFile)) {
            updateObjectDetailsInternal(inventory, inventoryDigest, inventoryStream, runnable);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    @Override
    public void deleteObjectDetails(String objectId) {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("DELETE FROM ocfl_object_details WHERE object_id = ?")) {
                statement.setString(1, objectId);
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateObjectDetailsInternal(Inventory inventory, String inventoryDigest, InputStream inventoryStream, Runnable runnable) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);

            try {
                if (storeInventory) {
                    insertWithInventory(connection, inventory, inventoryDigest, inventoryStream);
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

    private void insertWithInventory(Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream) throws SQLException {
        var previousRevision = previousRevisionIdStr(inventory.getRevisionId());

        var queryString = "INSERT INTO ocfl_object_details" +
                " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (object_id) DO UPDATE SET" +
                " (version_id, revision_id, inventory_digest, inventory, update_timestamp) =" +
                " (EXCLUDED.version_id, EXCLUDED.revision_id, EXCLUDED.inventory_digest, EXCLUDED.inventory, EXCLUDED.update_timestamp)" +
                " WHERE " +
                insertWhereClause(inventory, previousRevision);

        try (var updateStatement = connection.prepareStatement(queryString)) {
            updateStatement.setString(1, inventory.getId());
            updateStatement.setString(2, inventory.getHead().toString());
            updateStatement.setString(3, inventory.getObjectRootPath());
            updateStatement.setString(4, revisionIdStr(inventory.getRevisionId()));
            updateStatement.setString(5, inventoryDigest);
            updateStatement.setString(6, inventory.getDigestAlgorithm().getOcflName());
            updateStatement.setBinaryStream(7, inventoryStream);
            updateStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setString(9, previousVersionIdStr(inventory.getHead(), inventory.getRevisionId()));

            if (previousRevision != null) {
                updateStatement.setString(10, previousRevision);
            }

            verifyUpdate(updateStatement.executeUpdate(), inventory.getId());
        }
    }

    private void insertWithoutInventory(Connection connection, Inventory inventory, String inventoryDigest) throws SQLException {
        var previousRevision = previousRevisionIdStr(inventory.getRevisionId());

        var queryString = "INSERT INTO ocfl_object_details" +
                " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, update_timestamp)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?)" +
                " ON CONFLICT (object_id) DO UPDATE SET" +
                " (version_id, revision_id, inventory_digest, inventory, update_timestamp) =" +
                " (EXCLUDED.version_id, EXCLUDED.revision_id, EXCLUDED.inventory_digest, EXCLUDED.inventory, EXCLUDED.update_timestamp)" +
                " WHERE "
                + insertWhereClause(inventory, previousRevision);

        try (var updateStatement = connection.prepareStatement(queryString)) {
            updateStatement.setString(1, inventory.getId());
            updateStatement.setString(2, inventory.getHead().toString());
            updateStatement.setString(3, inventory.getObjectRootPath());
            updateStatement.setString(4, revisionIdStr(inventory.getRevisionId()));
            updateStatement.setString(5, inventoryDigest);
            updateStatement.setString(6, inventory.getDigestAlgorithm().getOcflName());
            updateStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            updateStatement.setString(8, previousVersionIdStr(inventory.getHead(), inventory.getRevisionId()));

            if (previousRevision != null) {
                updateStatement.setString(9, previousRevision);
            }

            verifyUpdate(updateStatement.executeUpdate(), inventory.getId());
        }
    }

    private String retrieveDigest(String objectId) {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement("SELECT inventory_digest FROM ocfl_object_details WHERE object_id = ?")) {
                statement.setString(1, objectId);

                try (var resultSet = statement.executeQuery()) {

                    if (resultSet.next()) {
                        return resultSet.getString(1);
                    }
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
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

    private String insertWhereClause(Inventory inventory, String previousRevision) {
        if (previousRevision != null) {
            return "ocfl_object_details.version_id = ? AND ocfl_object_details.revision_id = ?";
        } else if (inventory.getRevisionId() != null) {
            return "ocfl_object_details.version_id = ? AND ocfl_object_details.revision_id IS NULL";
        } else {
            return "(ocfl_object_details.version_id = ? AND ocfl_object_details.revision_id IS NULL)" +
                    " OR (ocfl_object_details.version_id = EXCLUDED.version_id AND ocfl_object_details.revision_id IS NOT NULL)";
        }
    }

    private void safeEnableAutoCommit(Connection connection) {
        try {
            connection.setAutoCommit(true);
        } catch (Exception e) {
            LOG.warn("Failed to enable autocommit", e);
        }
    }

}
