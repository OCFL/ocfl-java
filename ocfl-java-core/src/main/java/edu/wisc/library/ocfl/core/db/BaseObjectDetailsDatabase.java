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

package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
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
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

public abstract class BaseObjectDetailsDatabase implements ObjectDetailsDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(BaseObjectDetailsDatabase.class);

    private final DataSource dataSource;
    private final boolean storeInventory;
    private final long waitMillis;

    private final String lockFailCode;
    private final String duplicateKeyCode;

    public BaseObjectDetailsDatabase(DataSource dataSource, boolean storeInventory, long waitTime, TimeUnit timeUnit,
                                     String lockFailCode, String duplicateKeyCode) {
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        this.storeInventory = storeInventory;
        this.lockFailCode = Enforce.notBlank(lockFailCode, "lockFailCode cannot be blank");
        this.duplicateKeyCode = Enforce.notBlank(duplicateKeyCode, "duplicateKeyCode cannot be blank");
        this.waitMillis = timeUnit.toMillis(waitTime);
    }

    /**
     * Sets the amount of time to wait for a row lock before timing out.
     *
     * @param connection db connection
     * @param waitMillis time to wait for the lock in millis
     * @throws SQLException on sql error
     */
    protected abstract void setLockWaitTimeout(Connection connection, long waitMillis) throws SQLException;

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectDetails retrieveObjectDetails(String objectId) {
        Enforce.notBlank(objectId, "objectId cannot be blank");

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void addObjectDetails(Inventory inventory, String inventoryDigest, byte[] inventoryBytes) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notBlank(inventoryDigest, "inventoryDigest cannot be blank");
        Enforce.notNull(inventoryBytes, "inventoryBytes cannot be null");

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

    /**
     * {@inheritDoc}
     */
    @Override
    public void updateObjectDetails(Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notBlank(inventoryDigest, "inventoryDigest cannot be blank");
        Enforce.notNull(inventoryFile, "inventoryFile cannot be null");
        Enforce.notNull(runnable, "runnable cannot be null");

        try (var inventoryStream = Files.newInputStream(inventoryFile)) {
            updateObjectDetailsInternal(inventory, inventoryDigest, inventoryStream, runnable);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteObjectDetails(String objectId) {
        Enforce.notBlank(objectId, "objectId cannot be blank");

        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            setLockWaitTimeout(connection, waitMillis);

            try (var statement = connection.prepareStatement("DELETE FROM ocfl_object_details WHERE object_id = ?")) {
                statement.setString(1, objectId);
                statement.executeUpdate();
                connection.commit();
            } catch (Exception e) {
                connection.rollback();
                throw e;
            } finally {
                safeEnableAutoCommit(connection);
            }
        } catch (SQLException e) {
            throwLockException(e, objectId);
            throw new RuntimeException(e);
        }
    }

    private void updateObjectDetailsInternal(Inventory inventory, String inventoryDigest, InputStream inventoryStream, Runnable runnable) {
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            setLockWaitTimeout(connection, waitMillis);

            try {
                insertInventory(connection, inventory, inventoryDigest, inventoryStream);
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

    private void insertInventory(Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream) throws SQLException {
        try (var lockStatement = connection.prepareStatement("SELECT version_id, revision_id FROM ocfl_object_details WHERE object_id = ? FOR UPDATE")) {
            lockStatement.setString(1, inventory.getId());

            try (var lockResult = lockStatement.executeQuery()) {
                if (lockResult.next()) {
                    var existingVersionId = VersionId.fromString(lockResult.getString(1));
                    var existingRevisionId = revisionIdFromString(lockResult.getString(2));
                    verifyObjectDetailsState(existingVersionId, existingRevisionId, inventory);

                    executeUpdateDetails(connection, inventory, inventoryDigest, inventoryStream);
                } else {
                    executeInsertDetails(connection, inventory, inventoryDigest, inventoryStream);
                }
            }
        } catch (SQLException e) {
            throwLockException(e, inventory.getId());
            throw e;
        }
    }

    private void executeUpdateDetails(Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream) throws SQLException {
        try (var insertStatement = connection.prepareStatement("UPDATE ocfl_object_details SET" +
                " (version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)" +
                " = (?, ?, ?, ?, ?, ?, ?)" +
                " WHERE object_id = ?")) {
            insertStatement.setString(1, inventory.getHead().toString());
            insertStatement.setString(2, inventory.getObjectRootPath());
            insertStatement.setString(3, revisionIdStr(inventory.getRevisionId()));
            insertStatement.setString(4, inventoryDigest);
            insertStatement.setString(5, inventory.getDigestAlgorithm().getOcflName());
            if (storeInventory) {
                insertStatement.setBinaryStream(6, inventoryStream);
            } else {
                insertStatement.setNull(6, Types.BINARY);
            }
            insertStatement.setTimestamp(7, Timestamp.valueOf(LocalDateTime.now()));
            insertStatement.setString(8, inventory.getId());

            insertStatement.executeUpdate();
        }
    }

    private void executeInsertDetails(Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream) throws SQLException {
        try (var insertStatement = connection.prepareStatement("INSERT INTO ocfl_object_details" +
                " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)" +
                " VALUES (?, ?, ?, ?, ?, ?, ?, ?)")) {
            insertStatement.setString(1, inventory.getId());
            insertStatement.setString(2, inventory.getHead().toString());
            insertStatement.setString(3, inventory.getObjectRootPath());
            insertStatement.setString(4, revisionIdStr(inventory.getRevisionId()));
            insertStatement.setString(5, inventoryDigest);
            insertStatement.setString(6, inventory.getDigestAlgorithm().getOcflName());
            if (storeInventory) {
                insertStatement.setBinaryStream(7, inventoryStream);
            } else {
                insertStatement.setNull(7, Types.BINARY);
            }
            insertStatement.setTimestamp(8, Timestamp.valueOf(LocalDateTime.now()));

            insertStatement.executeUpdate();
        } catch (SQLException e) {
            if (duplicateKeyCode.equals(e.getSQLState())) {
                throw outOfSyncException(inventory.getId());
            }
            throw e;
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

    private String revisionIdStr(RevisionId revisionId) {
        return revisionId == null ? null : revisionId.toString();
    }

    private RevisionId revisionIdFromString(String revisionId) {
        if (revisionId == null) {
            return null;
        }
        return RevisionId.fromString(revisionId);
    }

    private void verifyObjectDetailsState(VersionId existingVersionId, RevisionId existingRevisionId, Inventory inventory) {
        if (existingRevisionId != null) {
            if (!Objects.equals(existingVersionId, inventory.getHead())) {
                throw outOfSyncException(inventory.getId());
            } else if (inventory.getRevisionId() != null
                    && !Objects.equals(existingRevisionId.nextRevisionId(), inventory.getRevisionId())) {
                throw outOfSyncException(inventory.getId());
            }
        } else {
            if (!Objects.equals(existingVersionId.nextVersionId(), inventory.getHead())) {
                throw outOfSyncException(inventory.getId());
            } else if (inventory.getRevisionId() != null && !Objects.equals(RevisionId.R1, inventory.getRevisionId())) {
                throw outOfSyncException(inventory.getId());
            }
        }
    }

    private ObjectOutOfSyncException outOfSyncException(String objectId) {
        throw new ObjectOutOfSyncException(String.format(
                "Cannot update object %s because its state is out of sync with the current state in the database.", objectId));
    }

    private void throwLockException(SQLException e, String objectId) {
        if (lockFailCode.equals(e.getSQLState())) {
            throw new LockException("Failed to acquire lock for object " + objectId);
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
