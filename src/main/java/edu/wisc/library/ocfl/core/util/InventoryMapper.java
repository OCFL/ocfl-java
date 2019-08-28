package edu.wisc.library.ocfl.core.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import edu.wisc.library.ocfl.core.model.Inventory;

import java.io.IOException;
import java.nio.file.Path;

/**
 * Wrapper around Jackson's ObjectMapper for serializing and deserializing Inventories. The ObjectMapper setup is a finicky
 * and I do not recommend changing it unless you are sure you know what you're doing.
 */
public class InventoryMapper {

    private ObjectMapper objectMapper;

    public InventoryMapper() {
        objectMapper = new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .configure(SerializationFeature.INDENT_OUTPUT, true)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
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
