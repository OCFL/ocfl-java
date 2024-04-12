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

import io.ocfl.api.exception.OcflDbException;
import io.ocfl.api.util.Enforce;
import io.ocfl.core.util.FileUtil;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Creates database tables if they don't exist.
 */
public class TableCreator {

    private static final Logger LOG = LoggerFactory.getLogger(TableCreator.class);

    private static final int MYSQL_ACCESS_DENIED_ERROR = 1142;
    private static final int MYSQL_NO_TABLE_ERROR = 1146;

    private static final String TABLE_TEST_QUERY = "SELECT 1 FROM %s LIMIT 1";

    private static final String LOCK_TABLE_FILE = "ocfl_object_lock.ddl.tmpl";
    private static final String OBJECT_DETAILS_TABLE_FILE = "ocfl_object_details.ddl.tmpl";

    private final Map<DbType, String> dbScriptDir = Map.of(
            DbType.POSTGRES, "db/postgresql",
            DbType.MARIADB, "db/mariadb",
            DbType.H2, "db/h2");

    private final DbType dbType;
    private final DataSource dataSource;

    public TableCreator(DbType dbType, DataSource dataSource) {
        this.dbType = Enforce.notNull(dbType, "dbType cannot be null");
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
    }

    public void createObjectLockTable(String tableName) {
        createTable(tableName, LOCK_TABLE_FILE);
    }

    public void createObjectDetailsTable(String tableName) {
        createTable(tableName, OBJECT_DETAILS_TABLE_FILE);
    }

    private void createTable(String tableName, String fileName) {
        Enforce.notBlank(tableName, "tableName cannot be blank");
        try (var connection = dataSource.getConnection()) {
            try {
                var filePath = getSqlFilePath(fileName);
                LOG.debug("Loading {}", filePath);
                if (filePath != null) {
                    try (var stream = this.getClass().getResourceAsStream("/" + filePath)) {
                        var ddlTemplate = streamToString(stream);
                        var ddl = String.format(ddlTemplate, tableName);
                        try (var statement = connection.prepareStatement(ddl)) {
                            statement.executeUpdate();
                        }
                    }
                }
            } catch (SQLException e) {
                // MySQL/MariaDB fail create if not exists queries when the user does not have permission even if the
                // table exists
                if (e.getErrorCode() == MYSQL_ACCESS_DENIED_ERROR) {
                    testTableExistence(connection, tableName);
                } else {
                    throw new OcflDbException(e);
                }
            }
        } catch (SQLException | IOException e) {
            throw new OcflDbException(e);
        }
    }

    private void testTableExistence(Connection connection, String tableName) {
        try (var statement = connection.prepareStatement(String.format(TABLE_TEST_QUERY, tableName))) {
            statement.execute();
        } catch (SQLException e) {
            if (e.getErrorCode() == MYSQL_NO_TABLE_ERROR) {
                throw new OcflDbException(String.format(
                        "Table %s does not exist and user does not have permission to create it.", tableName));
            }
            throw new OcflDbException(e);
        }
    }

    private String getSqlFilePath(String fileName) {
        var scriptDir = dbScriptDir.get(dbType);

        if (scriptDir == null) {
            LOG.warn("There are no scripts configured for {}", dbType);
            return null;
        } else {
            return FileUtil.pathJoinFailEmpty(scriptDir, fileName);
        }
    }

    private String streamToString(InputStream stream) throws IOException {
        return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
    }
}
