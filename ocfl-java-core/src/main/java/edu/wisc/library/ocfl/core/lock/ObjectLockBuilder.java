package edu.wisc.library.ocfl.core.lock;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.db.DbType;
import edu.wisc.library.ocfl.core.db.TableCreator;

import javax.sql.DataSource;
import java.util.concurrent.TimeUnit;

/**
 * Constructs new {@link ObjectLock} instances
 */
public class ObjectLockBuilder {

    private long waitTime;
    private TimeUnit timeUnit;

    public ObjectLockBuilder() {
        waitTime = 10;
        timeUnit = TimeUnit.SECONDS;
    }

    /**
     * Used to override the amount of time the client will wait to obtain an object lock. Default: 10 seconds.
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
     * Constructs a new {@link ObjectLock} that used the provided dataSource. If the database does not already contain
     * an object lock table, it attempts to create one.
     *
     * @param dataSource the connection to the database
     * @return database object lock
     */
    public ObjectLock buildDbLock(DataSource dataSource) {
        Enforce.notNull(dataSource, "dataSource cannot be null");

        var dbType = DbType.fromDataSource(dataSource);
        ObjectLock lock;

        switch (dbType) {
            case POSTGRES:
                lock = new PostgresObjectLock(dataSource, waitTime, timeUnit);
                break;
            default:
                throw new IllegalStateException(String.format("Database type %s is not mapped to an ObjectLock implementation.", dbType));
        }

        new TableCreator(dbType, dataSource).createObjectLockTable();

        return lock;
    }

    /**
     * Constructs a new in memory {@link ObjectLock}.
     *
     * @return in memory object lock
     */
    public ObjectLock buildMemLock() {
        return new InMemoryObjectLock(waitTime, timeUnit);
    }

}
