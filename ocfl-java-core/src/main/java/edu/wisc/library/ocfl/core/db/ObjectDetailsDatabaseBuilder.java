package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.util.Enforce;

import javax.sql.DataSource;

/**
 * Constructs {@link ObjectDetailsDatabase} instances
 */
public class ObjectDetailsDatabaseBuilder {

    private boolean storeInventory;

    public ObjectDetailsDatabaseBuilder() {
        storeInventory = true;
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
                database = new PostgresObjectDetailsDatabase(dataSource, storeInventory);
                break;
            default:
                throw new IllegalStateException(String.format("Database type %s is not mapped to an ObjectDetailsDatabase implementation.", dbType));
        }

        new TableCreator(dbType, dataSource).createObjectDetailsTable();

        return database;
    }

}
