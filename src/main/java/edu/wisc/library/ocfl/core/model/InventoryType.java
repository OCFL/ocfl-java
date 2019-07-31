package edu.wisc.library.ocfl.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum InventoryType {

    OCFL_1_0("https://ocfl.io/1.0/spec/#inventory");

    private final String id;

    private InventoryType(String id) {
        this.id = id;
    }

    @JsonValue
    public String getId() {
        return id;
    }

    @JsonCreator
    public static InventoryType fromValue(String value) {
        for (var entry : values()) {
            if (entry.id.equalsIgnoreCase(value)) {
                return entry;
            }
        }

        throw new IllegalArgumentException("Unknown InventoryType: " + value);
    }

}
