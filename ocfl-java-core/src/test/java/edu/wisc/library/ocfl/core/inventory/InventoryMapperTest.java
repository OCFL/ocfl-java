package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionNum;
import edu.wisc.library.ocfl.core.test.ITestHelper;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

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
        var digest = "abc123";
        var inventory = mapper.read(objectRoot, digest, new ByteArrayInputStream(original.getBytes()));
        assertFalse(inventory.hasMutableHead());
        assertNull(inventory.getRevisionNum());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
        assertEquals(objectRoot, inventory.getObjectRootPath());
        assertEquals(digest, inventory.getCurrentDigest());
    }

    @Test
    public void shouldRoundTripMutableHeadInventory() throws IOException {
        var original = readFile("simple-inventory.json");
        var objectRoot = "path/to/obj2";
        var digest = "def456";
        var revision = RevisionNum.fromString("r2");
        var inventory = mapper.readMutableHead(objectRoot, digest, revision, new ByteArrayInputStream(original.getBytes()));
        assertTrue(inventory.hasMutableHead());
        assertEquals(revision, inventory.getRevisionNum());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
        assertEquals(objectRoot, inventory.getObjectRootPath());
        assertEquals(digest, inventory.getCurrentDigest());
    }

    private String readFile(String name) throws IOException {
        return Files.readString(Paths.get("src/test/resources/other", name), StandardCharsets.UTF_8);
    }

    private String writeInventoryToString(Inventory inventory) {
        var outputStream = new ByteArrayOutputStream();
        mapper.write(outputStream, inventory);
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }

}
