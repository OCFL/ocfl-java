package edu.wisc.library.ocfl.core.util;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.MapperFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

public final class ObjectMappers {

    private ObjectMappers() {

    }

    /**
     * Default Jackson mapper configured to serialize OCFL model objects correctly. Not pretty printing.
     *
     * @return object mapper
     */
    public static ObjectMapper defaultMapper() {
        return new ObjectMapper()
                .registerModule(new JavaTimeModule())
                .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
                .disable(MapperFeature.CAN_OVERRIDE_ACCESS_MODIFIERS)
                .configure(MapperFeature.ALLOW_FINAL_FIELDS_AS_MUTATORS, false)
                .setSerializationInclusion(JsonInclude.Include.NON_NULL);
    }

    /**
     * The same as {@link #defaultMapper} except it pretty prints.
     *
     * @return object mapper
     */
    public static ObjectMapper prettyPrintMapper() {
        return defaultMapper().configure(SerializationFeature.INDENT_OUTPUT, true);
    }

}
