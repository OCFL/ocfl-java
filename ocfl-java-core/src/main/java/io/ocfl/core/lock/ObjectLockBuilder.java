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

package io.ocfl.core.lock;

import io.ocfl.api.util.Enforce;
import io.ocfl.core.db.DbType;
import io.ocfl.core.db.TableCreator;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;

/**
 * Constructs new {@link ObjectLock} instances
 */
public class ObjectLockBuilder {

    private static final String DEFAULT_TABLE_NAME = "ocfl_object_lock";

    private long waitTime;
    private TimeUnit timeUnit;
    private DataSource dataSource;
    private String tableName;
    private Duration maxLockDuration;

    public ObjectLockBuilder() {
        waitTime = 10;
        timeUnit = TimeUnit.SECONDS;
        maxLockDuration = Duration.ofHours(1);
    }

    /**
     * Used to override the amount of time the client will wait to obtain an object lock. Default: 10 seconds.
     *
     * <p>This only applies to in-memory locks
     *
     * @param waitTime wait time
     * @param timeUnit unit of time
     * @return builder
     */
    public ObjectLockBuilder waitTime(long waitTime, TimeUnit timeUnit) {
        this.waitTime = Enforce.expressionTrue(waitTime > 0, waitTime, "waitTime must be greater than 0");
        this.timeUnit = Enforce.notNull(timeUnit, "timeUnit cannot be null");
        return this;
    }

    /**
     * Sets the DataSource to use for DB based locking. This MUST be set in order to create a DB lock.
     *
     * @param dataSource the DataSource to use
     * @return builder
     */
    public ObjectLockBuilder dataSource(DataSource dataSource) {
        this.dataSource = dataSource;
        return this;
    }

    /**
     * Sets the name of the table to use for object locking. Default: ocfl_object_lock
     *
     * @param tableName the table name to use
     * @return builder
     */
    public ObjectLockBuilder tableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * Sets the maximum amount of time a lock may be held for before it's able to be acquired by another process.
     * Default: 1 hour
     *
     * <p>This only applies for database locks, and is used to avoid permanently locking an object if the process
     * that acquired the lock dies without releasing the lock. This duration should be fairly generous to allow
     * sufficient time for slow S3 writes.
     *
     * @param maxLockDuration the maximum amount of time a lock may be held for
     * @return builder
     */
    public ObjectLockBuilder maxLockDuration(Duration maxLockDuration) {
        this.maxLockDuration = maxLockDuration;
        return this;
    }

    /**
     * Constructs a new {@link ObjectLock}. If a DataSource was set, then a DB lock is created; otherwise, an in-memory
     * lock is used.
     *
     * @return object lock
     */
    public ObjectLock build() {
        if (dataSource == null) {
            return buildMemLock();
        }

        return buildDbLock();
    }

    private ObjectLock buildDbLock() {
        Enforce.notNull(dataSource, "dataSource cannot be null");

        var resolvedTableName = tableName == null ? DEFAULT_TABLE_NAME : tableName;

        var dbType = DbType.fromDataSource(dataSource);
        var lock = new DbObjectLock(dbType, resolvedTableName, dataSource, maxLockDuration);

        new TableCreator(dbType, dataSource).createObjectLockTable(resolvedTableName);

        return lock;
    }

    /**
     * Constructs a new in memory {@link ObjectLock}.
     *
     * @return in memory object lock
     */
    private ObjectLock buildMemLock() {
        return new InMemoryObjectLock(waitTime, timeUnit);
    }
}
