package edu.wisc.library.ocfl.core.inventory;

import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
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
        mapper = InventoryMapper.prettyPrintMapper();
    }

    @Test
    public void shouldRoundTripInventory() throws IOException {
        var original = readFile("simple-inventory.json");
        var inventory = mapper.read(new ByteArrayInputStream(original.getBytes()));
        assertFalse(inventory.hasMutableHead());
        assertNull(inventory.getRevisionId());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
    }

    @Test
    public void shouldRoundTripMutableHeadInventory() throws IOException {
        var original = readFile("simple-inventory.json");
        var revision = RevisionId.fromValue("r2");
        var inventory = mapper.readMutableHead(revision, new ByteArrayInputStream(original.getBytes()));
        assertTrue(inventory.hasMutableHead());
        assertEquals(revision, inventory.getRevisionId());
        var output = writeInventoryToString(inventory);
        assertEquals(original, output);
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
