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

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

public class H2ObjectDetailsDatabase extends BaseObjectDetailsDatabase {

    private static final String LOCK_FAIL_STATE = "HYT00";
    private static final String DUPLICATE_KEY_STATE = "23505";

    public H2ObjectDetailsDatabase(
            String tableName, DataSource dataSource, boolean storeInventory, long waitTime, TimeUnit timeUnit) {
        super(tableName, dataSource, storeInventory, waitTime, timeUnit, LOCK_FAIL_STATE);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void setLockWaitTimeout(Connection connection, long waitMillis) throws SQLException {
        try (var statement = connection.prepareStatement(String.format("SET LOCK_TIMEOUT %s", waitMillis))) {
            statement.executeUpdate();
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected boolean isConcurrentWriteException(SQLException exception) {
        return Objects.equals(exception.getSQLState(), DUPLICATE_KEY_STATE);
    }
}
