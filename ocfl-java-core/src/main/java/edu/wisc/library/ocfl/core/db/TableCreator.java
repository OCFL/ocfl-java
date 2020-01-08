package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.FileUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.Map;

/**
 * Creates database tables if they don't exist.
 */
public class TableCreator {

    private static final Logger LOG = LoggerFactory.getLogger(TableCreator.class);

    private static final String LOCK_TABLE_FILE = "ocfl_object_lock.sql";
    private static final String OBJECT_DETAILS_TABLE_FILE = "ocfl_object_details.sql";

    private Map<DbType, String> dbScriptDir = Map.of(
            DbType.POSTGRES, "db/postgresql"
    );

    private DbType dbType;
    private DataSource dataSource;

    public TableCreator(DbType dbType, DataSource dataSource) {
        this.dbType = Enforce.notNull(dbType, "dbType cannot be null");
        this.dataSource = Enforce.notNull(dataSource, "dataSource cannot be null");
    }

    public void createObjectLockTable() {
        createTable(LOCK_TABLE_FILE);
    }

    public void createObjectDetailsTable() {
        createTable(OBJECT_DETAILS_TABLE_FILE);
    }

    private void createTable(String fileName) {
        try (var connection = dataSource.getConnection()) {
            var filePath = getSqlFilePath(fileName);
            LOG.debug("Loading {}", filePath);
            if (filePath != null) {
                try (var stream = this.getClass().getResourceAsStream("/" + filePath)) {
                    try (var statement = connection.prepareStatement(streamToString(stream))) {
                        statement.executeUpdate();
                    }
                }
            }
        } catch (SQLException | IOException e) {
            throw new RuntimeException(e);
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
