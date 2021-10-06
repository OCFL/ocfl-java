package edu.wisc.library.ocfl.core.db;

import org.junit.Rule;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mariadb.jdbc.MariaDbDataSource;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class TableCreatorTest {

    @Rule
    public MariaDBContainer<?> mariaDB = new MariaDBContainer<>(DockerImageName.parse("mariadb:latest"));

    private TableCreator cut;
    private MariaDbDataSource dataSource;

    @BeforeEach
    void setUp() {
        dataSource = new MariaDbDataSource(mariaDB.getJdbcUrl());
        cut = new TableCreator(DbType.MARIADB, dataSource);
    }

    @Test
    void createObjectLockTable() {
        var tableName = "test_lock_table";
        cut.createObjectLockTable(tableName);
    }

    @Test
    void createObjectDetailsTable() {
    }
}
