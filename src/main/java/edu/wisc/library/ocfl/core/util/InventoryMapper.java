package edu.wisc.library.ocfl.core.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Wrapper around Jackson's ObjectMapper for serializing and deserializing Inventories. The ObjectMapper setup is a finicky
 * and I do not recommend changing it unless you are sure you know what you're doing.
 */
public class InventoryMapper {

    private ObjectMapper objectMapper;

    /**
     * Creates an InventoryMapper that will pretty print JSON files. This should be used when you value human readability
     * over disk space usage.
     */
    public static InventoryMapper prettyPrintMapper() {
        return new InventoryMapper(standardObjectMapper().configure(SerializationFeature.INDENT_OUTPUT, true));
    }

    /**
     * Creates an InventoryMapper that creates JSON files with as little whitespace as possible. This should be used when
     * minimizing disk space usage is more important than human readability.
     */
    public static InventoryMapper defaultMapper() {
        return new InventoryMapper(standardObjectMapper());
    }
    
    private static ObjectMapper standardObjectMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * Should use InventoryMapper.defaultMapper() or InventoryMapper.prettyPrintMapper() unless you know what you're doing.
     */
    public InventoryMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
    }

    public void writeValue(Path destination, Inventory inventory) {
        try {
            objectMapper.writeValue(destination.toFile(), inventory);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public Inventory readValue(Path path) {
        try {
            return objectMapper.readValue(path.toFile(), Inventory.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

}
