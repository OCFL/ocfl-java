package edu.wisc.library.ocfl.core.extension.layout.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The casing to use for hex encoded strings
 */
public enum Casing {

    UPPER("upper"),
    LOWER("lower");

    private String value;

    Casing(String value) {
        this.value = value;
    }

    @JsonCreator
    public static Casing fromString(String value) {
        for (var casing : values()) {
            if (casing.value.equalsIgnoreCase(value)) {
                return casing;
            }
        }

        throw new IllegalArgumentException("Unknown casing: " + value);
    }

    public boolean useUpperCase() {
        return this == UPPER;
    }

    public boolean useLowerCase() {
        return this == LOWER;
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
