/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.db;

import io.ocfl.api.exception.LockException;
import io.ocfl.api.exception.ObjectOutOfSyncException;
import io.ocfl.api.exception.OcflDbException;
import io.ocfl.api.exception.OcflIOException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.model.VersionNum;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.model.Inventory;
import io.ocfl.core.model.RevisionNum;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.sql.Types;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class BaseObjectDetailsDatabase implements ObjectDetailsDatabase {

    private static final Logger LOG = LoggerFactory.getLogger(BaseObjectDetailsDatabase.class);

    private final String tableName;
    private final DataSource dataSource;
    private final boolean storeInventory;
    private final long waitMillis;

    private final String lockFailCode;

    private final String selectDetailsQuery;
    private final String deleteDetailsQuery;
    private final String rowLockQuery;
    private final String updateDetailsQuery;
    private final String insertDetailsQuery;
    private final String selectDigestQuery;
    private final String deleteAllQuery;

    public BaseObjectDetailsDatabase(
            String tableName,
            DataSource dataSource,
            boolean storeInventory,
            long waitTime,
            TimeUnit timeUnit,
            String lockFailCode) {
        this.tableName = Enforce.notBlank(tableName, "tableName cannot be blank");
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        this.storeInventory = storeInventory;
        this.lockFailCode = Enforce.notBlank(lockFailCode, "lockFailCode cannot be blank");
        this.waitMillis = timeUnit.toMillis(waitTime);

        this.selectDetailsQuery = selectDetailsQuery(tableName);
        this.deleteDetailsQuery = deleteDetailsQuery(tableName);
        this.rowLockQuery = rowLockQuery(tableName);
        this.updateDetailsQuery = updateDetailsQuery(tableName);
        this.insertDetailsQuery = insertDetailsQuery(tableName);
        this.selectDigestQuery = selectDigestQuery(tableName);
        this.deleteAllQuery = deleteAllQuery(tableName);
    }

    /**
     * Sets the amount of time to wait for a row lock before timing out.
     * Keep in mind that MariaDB actually uses seconds for timeout, make sure that you
     * use at least 1000ms as input to the method and go up in 1 second increments.
     *
     * @param connection db connection
     * @param waitMillis time to wait for the lock in millis
     * @throws SQLException on sql error
     */
    protected abstract void setLockWaitTimeout(Connection connection, long waitMillis) throws SQLException;

    /**
     * Checks if given exception was thrown because of concurrent write issue or something else.
     * Check is performed by getting the SQL State Code from exception, but since each driver
     * and vendor uses their own codes, or even several codes this check needs to be driver specific.
     * H2 for example throws a "duplicate key" state code, while MariaDB can throw either "deadlock"
     * or "duplicate key" state codes.
     *
     * @param exception instance of an SQLException class to check
     * @return true if exception occurred because of concurrent write, false for any other exception
     */
    protected abstract boolean isConcurrentWriteException(SQLException exception);

    /**
     * Constructs the query for selecting object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String selectDetailsQuery(String tableName) {
        return String.format(
                "SELECT"
                        + " object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp"
                        + " FROM %s WHERE object_id = ?",
                tableName);
    }

    /**
     * Constructs the query for deleting object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String deleteDetailsQuery(String tableName) {
        return String.format("DELETE FROM %s WHERE object_id = ?", tableName);
    }

    /**
     * Constructs the query for locking object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String rowLockQuery(String tableName) {
        return String.format("SELECT version_id, revision_id FROM %s WHERE object_id = ? FOR UPDATE", tableName);
    }

    /**
     * Constructs the query for updating object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String updateDetailsQuery(String tableName) {
        return String.format(
                "UPDATE %s SET"
                        + " (version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)"
                        + " = (?, ?, ?, ?, ?, ?, ?)"
                        + " WHERE object_id = ?",
                tableName);
    }

    /**
     * Constructs the query for inserting object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String insertDetailsQuery(String tableName) {
        return String.format(
                "INSERT INTO %s"
                        + " (object_id, version_id, object_root_path, revision_id, inventory_digest, digest_algorithm, inventory, update_timestamp)"
                        + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                tableName);
    }

    /**
     * Constructs the query for selecting inventory digests
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String selectDigestQuery(String tableName) {
        return String.format("SELECT inventory_digest FROM %s WHERE object_id = ?", tableName);
    }

    /**
     * Constructs the query for deleting all object details entries
     *
     * @param tableName the name of the object details table
     * @return the query string
     */
    protected String deleteAllQuery(String tableName) {
        return String.format("DELETE FROM %s", tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public OcflObjectDetails retrieveObjectDetails(String objectId) {
        Enforce.notBlank(objectId, "objectId cannot be blank");

        OcflObjectDetails details = null;

        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement(selectDetailsQuery)) {
                statement.setString(1, objectId);

                try (var rs = statement.executeQuery()) {
                    if (rs.next()) {
                        details = new OcflObjectDetails()
                                .setObjectId(rs.getString(1))
                                .setVersionNum(VersionNum.fromString(rs.getString(2)))
                                .setObjectRootPath(rs.getString(3))
                                .setRevisionNum(revisionNumFromString(rs.getString(4)))
                                .setInventoryDigest(rs.getString(5))
                                .setDigestAlgorithm(DigestAlgorithm.fromOcflName(rs.getString(6)))
                                .setInventory(rs.getBytes(7))
                                .setUpdateTimestamp(rs.getTimestamp(8).toLocalDateTime());
                    }
                }
            }
        } catch (SQLException e) {
            throw new OcflDbException(e);
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
    public void updateObjectDetails(
            Inventory inventory, String inventoryDigest, Path inventoryFile, Runnable runnable) {
        Enforce.notNull(inventory, "inventory cannot be null");
        Enforce.notBlank(inventoryDigest, "inventoryDigest cannot be blank");
        Enforce.notNull(inventoryFile, "inventoryFile cannot be null");
        Enforce.notNull(runnable, "runnable cannot be null");

        try (var inventoryStream = new BufferedInputStream(Files.newInputStream(inventoryFile))) {
            updateObjectDetailsInternal(inventory, inventoryDigest, inventoryStream, runnable);
        } catch (IOException e) {
            throw new OcflIOException(e);
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

            try (var statement = connection.prepareStatement(deleteDetailsQuery)) {
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
            throw new OcflDbException(e);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void deleteAllDetails() {
        LOG.debug("Clearing all entries in the {} table", tableName);
        try (var connection = dataSource.getConnection()) {
            setLockWaitTimeout(connection, waitMillis);
            try (var statement = connection.prepareStatement(deleteAllQuery)) {
                statement.executeUpdate();
            }
        } catch (SQLException e) {
            throwLockException(e);
            throw new OcflDbException(e);
        }
    }

    private void updateObjectDetailsInternal(
            Inventory inventory, String inventoryDigest, InputStream inventoryStream, Runnable runnable) {
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
            throw new OcflDbException(e);
        }
    }

    private void insertInventory(
            Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream)
            throws SQLException {
        try (var lockStatement = connection.prepareStatement(rowLockQuery)) {
            lockStatement.setString(1, inventory.getId());

            try (var lockResult = lockStatement.executeQuery()) {
                if (lockResult.next()) {
                    var existingVersionNum = VersionNum.fromString(lockResult.getString(1));
                    var existingRevisionNum = revisionNumFromString(lockResult.getString(2));
                    verifyObjectDetailsState(existingVersionNum, existingRevisionNum, inventory);

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

    private void executeUpdateDetails(
            Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream)
            throws SQLException {
        try (var insertStatement = connection.prepareStatement(updateDetailsQuery)) {
            insertStatement.setString(1, inventory.getHead().toString());
            insertStatement.setString(2, inventory.getObjectRootPath());
            insertStatement.setString(3, revisionNumStr(inventory.getRevisionNum()));
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

    private void executeInsertDetails(
            Connection connection, Inventory inventory, String inventoryDigest, InputStream inventoryStream)
            throws SQLException {
        try (var insertStatement = connection.prepareStatement(insertDetailsQuery)) {
            insertStatement.setString(1, inventory.getId());
            insertStatement.setString(2, inventory.getHead().toString());
            insertStatement.setString(3, inventory.getObjectRootPath());
            insertStatement.setString(4, revisionNumStr(inventory.getRevisionNum()));
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
            if (isConcurrentWriteException(e)) {
                throw outOfSyncException(inventory.getId());
            }
            throw e;
        }
    }

    private String retrieveDigest(String objectId) {
        try (var connection = dataSource.getConnection()) {
            try (var statement = connection.prepareStatement(selectDigestQuery)) {
                statement.setString(1, objectId);

                try (var resultSet = statement.executeQuery()) {

                    if (resultSet.next()) {
                        return resultSet.getString(1);
                    }
                    return null;
                }
            }
        } catch (SQLException e) {
            throw new OcflDbException(e);
        }
    }

    private String revisionNumStr(RevisionNum revisionNum) {
        return revisionNum == null ? null : revisionNum.toString();
    }

    private RevisionNum revisionNumFromString(String revisionNum) {
        if (revisionNum == null) {
            return null;
        }
        return RevisionNum.fromString(revisionNum);
    }

    private void verifyObjectDetailsState(
            VersionNum existingVersionNum, RevisionNum existingRevisionNum, Inventory inventory) {
        if (existingRevisionNum != null) {
            if (!Objects.equals(existingVersionNum, inventory.getHead())) {
                throw outOfSyncException(inventory.getId());
            } else if (inventory.getRevisionNum() != null
                    && !Objects.equals(existingRevisionNum.nextRevisionNum(), inventory.getRevisionNum())) {
                throw outOfSyncException(inventory.getId());
            }
        } else {
            if (!Objects.equals(existingVersionNum.nextVersionNum(), inventory.getHead())) {
                throw outOfSyncException(inventory.getId());
            } else if (inventory.getRevisionNum() != null
                    && !Objects.equals(RevisionNum.R1, inventory.getRevisionNum())) {
                throw outOfSyncException(inventory.getId());
            }
        }
    }

    private ObjectOutOfSyncException outOfSyncException(String objectId) {
        throw new ObjectOutOfSyncException(String.format(
                "Cannot update object %s because its state is out of sync with the current state in the database.",
                objectId));
    }

    private void throwLockException(SQLException e, String objectId) {
        if (lockFailCode.equals(e.getSQLState())) {
            throw new LockException("Failed to acquire lock for object " + objectId);
        }
    }

    private void throwLockException(SQLException e) {
        if (lockFailCode.equals(e.getSQLState())) {
            throw new LockException("Failed to acquire lock: " + e.getMessage());
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
