package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.test.ITestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.*;

public class InventoryMapperTest {

    private static InventoryMapper mapper;

    @BeforeAll
    public static void setup() {
        mapper = ITestHelper.testInventoryMapper();
    }

    @Test
    public void shouldRoundTripInventory() throws IOException {
        var original = readFile("simple-inventory.json");
        var objectRoot = "path/to/obj1";
        var inventory = mapper.read(objectRoot, new ByteArrayInputStream(original.getBytes()));
        assertFalse(inventory.hasMutableHead());
        assertNull(inventory.getRevisionId());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
        assertEquals(objectRoot, inventory.getObjectRootPath());
    }

    @Test
    public void shouldRoundTripMutableHeadInventory() throws IOException {
        var original = readFile("simple-inventory.json");
        var objectRoot = "path/to/obj2";
        var revision = RevisionId.fromString("r2");
        var inventory = mapper.readMutableHead(objectRoot, revision, new ByteArrayInputStream(original.getBytes()));
        assertTrue(inventory.hasMutableHead());
        assertEquals(revision, inventory.getRevisionId());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
        assertEquals(objectRoot, inventory.getObjectRootPath());
    }

    private String readFile(String name) throws IOException {
        return new String(Files.readAllBytes(Paths.get("src/test/resources/other", name)));
    }

    private String writeInventoryToString(Inventory inventory) {
        var outputStream = new ByteArrayOutputStream();
        mapper.write(outputStream, inventory);
        return new String(outputStream.toByteArray());
    }

}
