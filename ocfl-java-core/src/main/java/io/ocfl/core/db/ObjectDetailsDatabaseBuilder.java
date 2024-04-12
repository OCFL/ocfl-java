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

import io.ocfl.api.exception.OcflJavaException;
import io.ocfl.api.util.Enforce;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Constructs {@link ObjectDetailsDatabase} instances
 */
public class ObjectDetailsDatabaseBuilder {

    private static final String DEFAULT_TABLE_NAME = "ocfl_object_details";

    private boolean storeInventory;
    private long waitTime;
    private TimeUnit timeUnit;
    private DataSource dataSource;
    private String tableName;

    public ObjectDetailsDatabaseBuilder() {
        storeInventory = true;
        waitTime = 10;
        timeUnit = TimeUnit.SECONDS;
    }

    /**
     * If serialized inventories should be stored in the database. Default: true.
     *
     * @param storeInventory true if serialized inventories should be stored in the database.
     * @return builder
     */
    public ObjectDetailsDatabaseBuilder storeInventory(boolean storeInventory) {
        this.storeInventory = storeInventory;
        return this;
    }

    /**
     * Used to override the amount of time the client will wait to obtain a lock. Default: 10 seconds.
     *
     * @param waitTime wait time (MariaDB uses seconds, while PostgreSQL and H2 use milliseconds)
     * @param timeUnit unit of time
     * @return builder
     */
    public ObjectDetailsDatabaseBuilder waitTime(long waitTime, TimeUnit timeUnit) {
        this.waitTime = Enforce.expressionTrue(waitTime > 0, waitTime, "waitTime must be greater than 0");
        this.timeUnit = Enforce.notNull(timeUnit, "timeUnit cannot be null");
        return this;
    }

    /**
     * Sets the DataSource to use for the object details table. This is a required field.
     *
     * @param dataSource the DataSource
     * @return builder
     */
    public ObjectDetailsDatabaseBuilder dataSource(DataSource dataSource) {
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
        return this;
    }

    /**
     * Sets the name of the table to use to store object details. Default: ocfl_object_details
     *
     * @param tableName the table name to use
     * @return builder
     */
    public ObjectDetailsDatabaseBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Constructs a new {@link ObjectDetailsDatabase} instance using the given dataSource. If the database does not
     * already contain an object details table, it attempts to create one.
     *
     * @return ObjectDetailsDatabase
     */
    public ObjectDetailsDatabase build() {
        Enforce.notNull(dataSource, "dataSource cannot be null");

        var resolvedTableName = tableName == null ? DEFAULT_TABLE_NAME : tableName;

        var dbType = DbType.fromDataSource(dataSource);
        ObjectDetailsDatabase database;

        switch (dbType) {
            case POSTGRES:
                database = new PostgresObjectDetailsDatabase(
                        resolvedTableName, dataSource, storeInventory, waitTime, timeUnit);
                break;
            case MARIADB:
                database = new MariaDbObjectDetailsDatabase(
                        resolvedTableName, dataSource, storeInventory, waitTime, timeUnit);
                break;
            case H2:
                database =
                        new H2ObjectDetailsDatabase(resolvedTableName, dataSource, storeInventory, waitTime, timeUnit);
                break;
            default:
                throw new OcflJavaException(String.format(
                        "Database type %s is not mapped to an ObjectDetailsDatabase implementation.", dbType));
        }

        new TableCreator(dbType, dataSource).createObjectDetailsTable(resolvedTableName);

        return database;
    }
}
