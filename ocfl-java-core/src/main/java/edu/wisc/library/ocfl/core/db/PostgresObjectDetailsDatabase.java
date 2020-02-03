package edu.wisc.library.ocfl.core.db;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.TimeUnit;

public class PostgresObjectDetailsDatabase extends BaseObjectDetailsDatabase {

    private static final String LOCK_FAIL_STATE = "55P03";
    private static final String DUPLICATE_KEY_STATE = "23505";

    public PostgresObjectDetailsDatabase(DataSource dataSource, boolean storeInventory, long waitTime, TimeUnit timeUnit) {
        super(dataSource, storeInventory, waitTime, timeUnit, LOCK_FAIL_STATE, DUPLICATE_KEY_STATE);
    }

    protected void setLockWaitTimeout(Connection connection, long waitMillis) throws SQLException {
        try (var statement = connection.prepareStatement(String.format("SET LOCAL lock_timeout = %s", waitMillis))) {
            statement.executeUpdate();
        }
    }

}