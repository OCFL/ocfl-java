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

package edu.wisc.library.ocfl.core.lock;

import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.exception.OcflDbException;
import edu.wisc.library.ocfl.api.exception.OcflJavaException;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.db.DbType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Map;
import java.util.concurrent.Callable;

/**
 * Lock implementation that writes to a DB table to lock a resource. The row is deleted when the lock is released.
 * The lock expires if the row has not been deleted within a configurable period of time.
 */
public class DbObjectLock implements ObjectLock {

    private static final Logger LOG = LoggerFactory.getLogger(DbObjectLock.class);

    private static final Map<DbType, String> DUPLICATE_STATE_CODES = Map.of(
            DbType.H2, "23505",
            DbType.MARIADB, "23000",
            DbType.POSTGRES, "23505"
    );

    private final String tableName;
    private final DataSource dataSource;
    private final Duration lockDuration;

    private final String createRowLockQuery;
    private final String updateRowLockQuery;
    private final String deleteRowLockQuery;

    private final String duplicateStateCode;

    public DbObjectLock(DbType dbType, String tableName, DataSource dataSource, Duration maxLockDuration) {
        Enforce.notNull(dbType, "dbType cannot be null");
        this.tableName = Enforce.notBlank(tableName, "tableName cannot be blank");
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        this.lockDuration = Enforce.notNull(maxLockDuration, "maxLockDuration cannot be null");

        this.duplicateStateCode = Enforce.notBlank(DUPLICATE_STATE_CODES.get(dbType), "duplicate state code cannot be blank");

        this.createRowLockQuery = String.format("INSERT INTO %s (object_id, acquired_timestamp) VALUES (?, ?)", tableName);
        this.updateRowLockQuery = String.format("UPDATE %s SET acquired_timestamp = ? WHERE object_id = ? AND acquired_timestamp <= ?", tableName);
        this.deleteRowLockQuery = String.format("DELETE FROM %s WHERE object_id = ? AND acquired_timestamp = ?", tableName);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void doInWriteLock(String objectId, Runnable doInLock) {
        doInWriteLock(objectId, () -> {
            doInLock.run();
            return null;
        });
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <T> T doInWriteLock(String objectId, Callable<T> doInLock) {
        var now = Instant.now().truncatedTo(ChronoUnit.MILLIS);

        try (var connection = dataSource.getConnection()) {
            if (!createLockRow(objectId, now, connection)) {
                // Try acquire twice to cover the case where the lock is released when the first UPDATE
                // was attempted. More retries are possible, but you have to draw the line somewhere.
                if (!createLockRow(objectId, now, connection)) {
                    throw  failedToAcquireLock(objectId);
                }
            }
        } catch (SQLException e) {
            throw new OcflDbException(e);
        }

        try {
            return doInLock.call();
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new OcflJavaException(e);
        } finally {
            releaseLock(objectId, now);
        }
    }

    private boolean createLockRow(String objectId, Instant timestamp, Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement(createRowLockQuery)) {
            statement.setString(1, objectId);
            statement.setTimestamp(2, Timestamp.from(timestamp));
            statement.executeUpdate();
            return true;
        } catch (SQLException e) {
            if (duplicateStateCode.equals(e.getSQLState())) {
                // this happens when there is already a lock entry for the object, but the lock could be expired
                return updateLockRow(objectId, timestamp, connection);
            }
            throw e;
        }
    }

    private boolean updateLockRow(String objectId, Instant timestamp, Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement(updateRowLockQuery)) {
            var expired = timestamp.minus(lockDuration);
            statement.setTimestamp(1, Timestamp.from(timestamp));
            statement.setString(2, objectId);
            statement.setTimestamp(3, Timestamp.from(expired));

            var updateCount = statement.executeUpdate();

            return updateCount == 1;
        }
    }

    private LockException failedToAcquireLock(String objectId) {
        return new LockException("Failed to acquire lock for object " + objectId);
    }

    private void releaseLock(String objectId, Instant timestamp) {
        try (var connection = dataSource.getConnection();
             var statement = connection.prepareStatement(deleteRowLockQuery)) {
            statement.setString(1, objectId);
            statement.setTimestamp(2, Timestamp.from(timestamp));
            statement.executeUpdate();
        } catch (SQLException e) {
            LOG.error("Failed to release lock on object {}", objectId, e);
        }
    }

}
