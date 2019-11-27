package edu.wisc.library.ocfl.core.extension.layout.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The type of encapsulation to use
 */
public enum EncapsulationType {

    /**
     * Use the object id as the encapsulation directory
     */
    ID("id"),
    /**
     * Use a number of characters from the end of the encoded object id as the encapsulation directory
     */
    SUBSTRING("substring");

    private String value;

    EncapsulationType(String value) {
        this.value = value;
    }

    @JsonCreator
    public static EncapsulationType fromString(String value) {
        for (var type : values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown encapsulation type: " + value);
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }

}
