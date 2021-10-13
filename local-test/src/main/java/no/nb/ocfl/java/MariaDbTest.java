package no.nb.ocfl.java;

import edu.wisc.library.ocfl.api.OcflConfig;
import edu.wisc.library.ocfl.core.db.MariaDbObjectDetailsDatabase;
import edu.wisc.library.ocfl.core.db.ObjectDetailsDatabaseBuilder;
import edu.wisc.library.ocfl.core.inventory.InventoryMapper;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.Version;
import edu.wisc.library.ocfl.core.util.DigestUtil;
import org.mariadb.jdbc.MariaDbDataSource;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.OffsetDateTime;
import java.util.Scanner;
import java.util.concurrent.Phaser;
import java.util.concurrent.TimeUnit;

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

    private static final Scanner scanner = new Scanner(System.in);
    private static final String CONNECTION_STRING = "jdbc:mariadb://localhost:3306/ocfl?user=root&password=root";
    private static final String OBJECT_ID = "test_obj_id";

    private final MariaDbObjectDetailsDatabase database;

    private void addDetails() {
        addObjects(OBJECT_ID);
    }

    private void updateDetails() {
        addDetails();

        var inventory = basicInventory(OBJECT_ID).buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f2", "file2.txt")
                        .message("Basic update test")
                        .build())
                .build();

        database.updateObjectDetails(inventory,
                DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), inventoryBytes(inventory)),
                writeInventory(inventoryBytes(inventory)), () -> {
                });
    }

    private void deleteDetails() {
        updateDetails();
        database.deleteObjectDetails(OBJECT_ID);
    }

    private void deleteSeveralDetails() {
        // Add three objects to the database in addition to regular test object
        updateDetails();
        var id1 = "extra_obj_1";
        var id2 = "extra_obj_2";
        var id3 = "extra_obj_3";
        addObjects(id1, id2, id3);

        // List objects in database
        System.out.println("Here are the files in database:");
        listDetail(OBJECT_ID, id1, id2, id3);

        // Let user inspect database if they wish to
        askForInput("Press enter to continue");

        // Delete details in database
        database.deleteAllDetails();

        // Show the results
        System.out.println("After the delete:");
        listDetail(OBJECT_ID, id1, id2, id3);
    }

    private void createLockError() {
        // Add a single details file
        addDetails();

        // Create update data
        var inventory = basicInventory(OBJECT_ID).buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .message("Testing locking error")
                        .build())
                .build();
        var invBytes = inventoryBytes(inventory);
        var invPath = writeInventory(invBytes);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        // Set up phaser for synchronizing threads
        var phaser = new Phaser(2);

        // Start a thread that will get a lock and wait
        System.out.println("Starting a thread that will acquire a lock and keep it");
        new Thread(() -> database.updateObjectDetails(inventory, digest, invPath, phaser::arriveAndAwaitAdvance)).start();

        // Wait a bit and start a new thread that will try to acquire the same lock, but fail
        try {
            TimeUnit.MILLISECONDS.sleep(100);
            System.out.println("Started a second update, waiting for timeout and lock exception to happen");
            database.updateObjectDetails(inventory, digest, invPath, () -> {
            });
        } catch (Exception e) {
            System.out.printf("Got exception of type: %s, with following message: %s%n", e.getClass().getName(), e.getMessage());
        }

        // Let the thread finish its execution
        System.out.println("Resuming the first update operation");
        phaser.arriveAndAwaitAdvance();
    }

    private void createOutOfSync() {
        // Add a single details file
        addDetails();

        // Create update data
        var inventory = basicInventory(OBJECT_ID).buildFrom()
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file2.txt")
                        .message("Testing out of sync error")
                        .build())
                .build();
        var invBytes = inventoryBytes(inventory);
        var invPath = writeInventory(invBytes);
        var digest = DigestUtil.computeDigestHex(inventory.getDigestAlgorithm(), invBytes);

        // Start two updates last one will fail
        try {
            database.updateObjectDetails(inventory, digest, invPath, () -> {
            });
            database.updateObjectDetails(inventory, digest, invPath, () -> {
            });
        } catch (Exception e) {
            System.out.printf("Got exception of type: %s, with following message: %s%n", e.getClass().getName(), e.getMessage());
        }
    }

    public MariaDbTest() {
        MariaDbDataSource dataSource = new MariaDbDataSource(CONNECTION_STRING);
        database = (MariaDbObjectDetailsDatabase) new ObjectDetailsDatabaseBuilder().dataSource(dataSource).waitTime(2, TimeUnit.SECONDS).build();
    }

    public static void main(String[] args) {
        var test = new MariaDbTest();

        while (true) {
            test.database.deleteAllDetails();
            displayMenu();
            var choice = askForInput();

            switch (choice) {
                case "1":
                    printTestHeader("----------------------- Add a details object ------------------------");
                    test.addDetails();
                    break;
                case "2":
                    printTestHeader("---------------------- Update a details object ----------------------");
                    test.updateDetails();
                    break;
                case "3":
                    printTestHeader("---------------------- Delete a details object ----------------------");
                    test.deleteDetails();
                    break;
                case "4":
                    printTestHeader("------------------------- Delete all objects ------------------------");
                    test.deleteSeveralDetails();
                    break;
                case "5":
                    printTestHeader("------------------------ Create a lock error ------------------------");
                    test.createLockError();
                    break;
                case "6":
                    printTestHeader("-------------------- Create an out of sync error --------------------");
                    test.createOutOfSync();
                    break;
                default:
                    test.database.deleteAllDetails();
                    System.out.println("★·.·´¯`·.·´¯`·.·★ ᕙ(^▿^-ᕙ) ★  Bye Bye  ★ (ᕗ-^▿^)ᕗ ★·.·´¯`·.·´¯`·.·★");
                    return;
            }

            System.out.printf("Object with ID: '%s' after test:%n", OBJECT_ID);
            test.listDetail();
            askForInput("%n++++++++++++++++++++++ Press ENTER to continue ++++++++++++++++++++++");
        }
    }

    //////////////////////////////////////
    //// Helper methods for the tests ////
    //////////////////////////////////////

    private static void displayMenu() {
        System.out.println();
        printChars('%', 69);
        System.out.println("Menu for test program:");
        System.out.println("1. ------------------------------ Add a single object to the database");
        System.out.println("2. --------------------------- Update a single object in the database");
        System.out.println("3. ------------------------- Delete a single object from the database");
        System.out.println("4. ------------------------- Delete several objects from the database");
        System.out.println("5. -------------------------------------------- Test a lock exception");
        System.out.println("6. ----------------------------- Test an object out of sync exception");
        System.out.println("e. ------------------------------------------------- Exit application");
        System.out.printf("------------------------------------------------------ Your choice? ");
    }

    private static void printTestHeader(String testName) {
        printChars('%', 69);
        System.out.println(testName);
    }

    private static void printChars(char symbol, int times) {
        printChars(symbol, times, true);
    }

    private static void printChars(char symbol, int times, boolean lineShift) {
        for (int i = 0; i++ < times; System.out.printf("%s", symbol));
        if (lineShift) System.out.println();
    }

    private void listDetail() {
        listDetail(OBJECT_ID);
    }

    private void listDetail(String... ids) {
        System.out.println();
        printChars('~', 69, false);
        for (var id : ids) {
            System.out.println();
            var details = database.retrieveObjectDetails(id);
            if (details == null) {
                System.out.printf("xxxxx No object with ID '%s' was found in the database xxxxx%n", id);
            } else {
                System.out.printf("Object ID:        '%.47s'%n", details.getObjectId());
                System.out.printf("Version Number:   '%.47s'%n", details.getVersionNum());
                System.out.printf("Revision Number:  '%.47s'%n", details.getRevisionNum());
                System.out.printf("Object Root Path: '%.47s'%n", details.getObjectRootPath());
                System.out.printf("Inventory Digest: '%.47s...'%n", details.getInventoryDigest());
                System.out.printf("Digest Algorithm: '%.47s'%n", details.getDigestAlgorithm().getOcflName());
                System.out.printf("Update Timestamp: '%.47s'%n", details.getUpdateTimestamp());
            }
        }
        printChars('~', 69);
    }

    private static String askForInput() {
        return askForInput("");
    }

    private static String askForInput(String message) {
        System.out.printf(message);

        return scanner.nextLine().trim().toLowerCase();
    }

    private Inventory basicInventory(String objectId) {
        return Inventory.builderFromStub(objectId, new OcflConfig(), "o1")
                .addFileToManifest("f1", "v1/content/file1.txt")
                .addHeadVersion(Version.builder()
                        .created(OffsetDateTime.now())
                        .addFile("f1", "file1.txt")
                        .build())
                .build();
    }

    private byte[] inventoryBytes(Inventory inventory) {
        var stream = new ByteArrayOutputStream();

        InventoryMapper.prettyPrintMapper().write(stream, inventory);

        return stream.toByteArray();
    }

    private Path writeInventory(byte[] invBytes) {
        try {
            new File("inventory.json").createNewFile();
            var dst = Paths.get("inventory.json");
            Files.write(dst, invBytes);
            new File("inventory.json").deleteOnExit();
            return dst;
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }
    }

    private void addObjects(String... ids) {
        for (var id: ids) {
            var i1 = basicInventory(id);
            database.addObjectDetails(i1, DigestUtil.computeDigestHex(i1.getDigestAlgorithm(), inventoryBytes(i1)), inventoryBytes(i1));
        }
    }
}
