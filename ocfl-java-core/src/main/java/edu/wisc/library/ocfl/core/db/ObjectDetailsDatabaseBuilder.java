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

import edu.wisc.library.ocfl.api.exception.OcflJavaException;
import edu.wisc.library.ocfl.api.util.Enforce;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

/**
 * Constructs {@link ObjectDetailsDatabase} instances
 */
public class ObjectDetailsDatabaseBuilder {

    private boolean storeInventory;
    private long waitTime;
    private TimeUnit timeUnit;

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
     * @param waitTime wait time
     * @param timeUnit unit of time
     * @return builder
     */
    public ObjectDetailsDatabaseBuilder waitTime(long waitTime, TimeUnit timeUnit) {
        this.waitTime = Enforce.expressionTrue(waitTime > 0, waitTime, "waitTime must be greater than 0");
        this.timeUnit = Enforce.notNull(timeUnit, "timeUnit cannot be null");
        return this;
    }

    /**
     * Constructs a new {@link ObjectDetailsDatabase} instance using the given dataSource. If the database does not
     * already contain an object details table, it attempts to create one.
     *
     * @param dataSource the connection to the database
     * @return ObjectDetailsDatabase
     */
    public ObjectDetailsDatabase build(DataSource dataSource) {
        Enforce.notNull(dataSource, "dataSource cannot be null");

        var dbType = DbType.fromDataSource(dataSource);
        ObjectDetailsDatabase database;

        switch (dbType) {
            case POSTGRES:
                database = new PostgresObjectDetailsDatabase(dataSource, storeInventory, waitTime, timeUnit);
                break;
            case H2:
                database = new H2ObjectDetailsDatabase(dataSource, storeInventory, waitTime, timeUnit);
                break;
            default:
                throw new OcflJavaException(String.format("Database type %s is not mapped to an ObjectDetailsDatabase implementation.", dbType));
        }

        new TableCreator(dbType, dataSource).createObjectDetailsTable();

        return database;
    }

}
