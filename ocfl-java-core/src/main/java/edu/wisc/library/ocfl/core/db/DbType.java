package edu.wisc.library.ocfl.core.db;

import javax.sql.DataSource;
import java.sql.SQLException;

/**
 * This enum describes the database types that the library supports out of the box.
 */
public enum DbType {

    POSTGRES("PostgreSQL");

    private String productName;

    DbType(String productName) {
        this.productName = productName;
    }

    public static DbType fromDataSource(DataSource dataSource) {
        try (var connection = dataSource.getConnection()) {
            var productName = connection.getMetaData().getDatabaseProductName();

            for (var type : values()) {
                if (type.productName.equals(productName)) {
                    return type;
                }
            }

            throw new IllegalArgumentException(String.format("%s is not mapped to a DbType.", productName));
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

}
