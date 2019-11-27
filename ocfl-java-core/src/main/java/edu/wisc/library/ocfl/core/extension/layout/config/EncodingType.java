package edu.wisc.library.ocfl.core.extension.layout.config;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

/**
 * The encoding to use on the object id
 */
public enum EncodingType {

    // TODO encryption?
    /**
     * The object id is not encode -- can result in invalid paths
     */
    NONE("none"),
    /**
     * The object id is hashed. A digest algorithm must be specified.
     */
    HASH("hash"),
    /**
     * The object id is url encoded.
     */
    URL("url"),
    /**
     * The object id is pairtree cleaned.
     */
    PAIRTREE("pairtree");

    private String value;

    EncodingType(String value) {
        this.value = value;
    }

    @JsonCreator
    public static EncodingType fromString(String value) {
        for (EncodingType type : values()) {
            if (type.value.equalsIgnoreCase(value)) {
                return type;
            }
        }

        throw new IllegalArgumentException("Unknown encoding: " + value);
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
