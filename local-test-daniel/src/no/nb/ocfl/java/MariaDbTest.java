package edu.wisc.library.ocfl.core.db;

import edu.wisc.library.ocfl.core.db.MariaDbObjectDetailsDatabase;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabaseBuilder;
import org.mariadb.jdbc.MariaDbDataSource;

public class MariaDbTest {
    /* Start the MariaDB database in docker using the following:
     * $  docker network create ocfl-network
     * $  docker run --net ocfl-network -h "0.0.0.0" -p 3306:3306 --name ocfl-test -e MARIADB_ROOT_PASSWORD=root -d mariadb:latest
     * You can test if it's running by connecting to the server and creating a database:
     * $  mariadb --host 127.0.0.1 -P 3306 --user root -pocfl-test-password
     * $  CREATE DATABASE IF NOT EXISTS ocfl;
     *
     * If you want to see the database easily for testing, run a local phpMyAdmin image in docker
     * $  docker run --net ocfl-network --name php-my-admin -d -e PMA_ARBITRARY=1 -p 8080:80 phpmyadmin:latest
     * Then go to localhost:8080 in your browser and provide 'ocfl-test:3306' as server, 'root' as user and password
     */

    private static MariaDbObjectDetailsDatabase database;
    private static MariaDbDataSource dataSource;
    private static String connectionString = "jdbc:mariadb://localhost:3306/ocfl?user=root&password=root";

    public static void main(String[] args) {
        dataSource = new MariaDbDataSource(connectionString);
        database = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder().dataSource(dataSource).build();
    }
}
