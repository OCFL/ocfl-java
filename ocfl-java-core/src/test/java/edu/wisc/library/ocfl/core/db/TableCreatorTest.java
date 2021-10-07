package edu.wisc.library.ocfl.core.db;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import java.sql.SQLException;
import java.util.ArrayList;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TableCreatorTest {

    @Container
    private final MariaDBContainer<?> mariaDB = new MariaDBContainer<>(DockerImageName.parse("mariadb:latest"));

    private TableCreator cut;
    private MariaDbDataSource dataSource;

    @BeforeEach
    void beforeEach() throws SQLException {
        dataSource = new MariaDbDataSource(mariaDB.getJdbcUrl());
        dataSource.setUser(mariaDB.getUsername());
        dataSource.setPassword(mariaDB.getPassword());

        cut = new TableCreator(DbType.MARIADB, dataSource);
    }

    @Test
    void createObjectLockTable() throws SQLException {
        var tableName = "test_lock_table";
        cut.createObjectLockTable(tableName);

        var result = dataSource.getConnection().prepareStatement("DESCRIBE " + tableName).executeQuery();
        var names = new ArrayList<String>();
        while (result.next()) {
            names.add(result.getString("Field"));
        }

        assertTrue(names.contains("object_id"), "Row 'object_id' was not created for table: " + tableName);
        assertTrue(names.contains("acquired_timestamp"), "Row 'acquired_timestamp' was not created for table: " + tableName);
    }

    @Test
    void createObjectDetailsTable() throws SQLException {
        var tableName = "test_details_table";
        cut.createObjectDetailsTable(tableName);

        var result = dataSource.getConnection().prepareStatement("DESCRIBE " + tableName).executeQuery();
        var names = new ArrayList<String>();
        while (result.next()) {
            names.add(result.getString("Field"));
        }

        assertTrue(names.contains("object_id"), "Row 'object_id' was not created for table: " + tableName);
        assertTrue(names.contains("version_id"), "Row 'version_id' was not created for table: " + tableName);
        assertTrue(names.contains("object_root_path"), "Row 'object_root_path' was not created for table: " + tableName);
        assertTrue(names.contains("revision_id"), "Row 'revision_id' was not created for table: " + tableName);
        assertTrue(names.contains("inventory_digest"), "Row 'inventory_digest' was not created for table: " + tableName);
        assertTrue(names.contains("digest_algorithm"), "Row 'digest_algorithm' was not created for table: " + tableName);
        assertTrue(names.contains("update_timestamp"), "Row 'update_timestamp' was not created for table: " + tableName);
        assertTrue(names.contains("inventory"), "Row 'inventory' was not created for table: " + tableName);
    }
}
