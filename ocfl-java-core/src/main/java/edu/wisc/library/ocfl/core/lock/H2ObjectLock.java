package edu.wisc.library.ocfl.core.lock;

import edu.wisc.library.ocfl.api.exception.LockException;
import edu.wisc.library.ocfl.api.util.Enforce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

public class H2ObjectLock implements ObjectLock {

    private static final Logger LOG = LoggerFactory.getLogger(H2ObjectLock.class);

    private static final String OBJECT_LOCK_FAIL = "HYT00";

    private DataSource dataSource;
    private long waitMillis;

    public H2ObjectLock(DataSource dataSource, long waitTime, TimeUnit timeUnit) {
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        Enforce.expressionTrue(waitTime > -1, waitTime, "waitTime cannot be negative");
        Enforce.notNull(timeUnit, "timeUnit cannot be null");
        this.waitMillis = timeUnit.toMillis(waitTime);
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
        try (var connection = dataSource.getConnection()) {
            connection.setAutoCommit(false);
            setLockWaitTimeout(connection);

            try {
                createLockRow(objectId, connection);

                try (var statement = acquireLock(connection)) {
                    statement.setString(1, objectId);

                    try (var resultSet = statement.executeQuery()) {
                        if (resultSet.next()) {
                            return doInLock.call();
                        } else {
                            throw failedToAcquireLock(objectId);
                        }
                    }
                }
            } catch (SQLException e) {
                if (OBJECT_LOCK_FAIL.equals(e.getSQLState())) {
                    throw failedToAcquireLock(objectId);
                }
                throw e;
            } finally {
                safeCleanup(connection);
            }
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private void createLockRow(String objectId, Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement("MERGE INTO ocfl_object_lock" +
                     " (object_id) VALUES (?)")) {
            statement.setString(1, objectId);
            statement.executeUpdate();
        }
    }

    private void setLockWaitTimeout(Connection connection) throws SQLException {
        try (var statement = connection.prepareStatement(String.format("SET LOCK_TIMEOUT %s", waitMillis))) {
            statement.executeUpdate();
        }
    }

    private LockException failedToAcquireLock(String objectId) {
        return new LockException("Failed to acquire lock for object " + objectId);
    }

    private PreparedStatement acquireLock(Connection connection) throws SQLException {
        return connection.prepareStatement("SELECT object_id FROM ocfl_object_lock WHERE object_id = ? FOR UPDATE");
    }

    private void safeCleanup(Connection connection) {
        try {
            connection.commit();
        } catch (Exception e) {
            LOG.warn("Failed to commit", e);
        }

        try {
            connection.setAutoCommit(true);
        } catch (Exception e) {
            LOG.warn("Failed to enable autocommit", e);
        }
    }

}
