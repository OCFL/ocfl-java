package edu.wisc.library.ocfl.core.extension.layout;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.core.extension.layout.config.FlatLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.NTupleLayoutConfig;

/**
 * Mapping of layout extensions to their application configuration
 */
public enum LayoutExtension {

    // TODO these keys are made up
    FLAT("layout-flat", FlatLayoutConfig.class),
    N_TUPLE("layout-n-tuple", NTupleLayoutConfig.class);

    private String key;
    private Class<? extends LayoutConfig> configClass;

    LayoutExtension(String key, Class<? extends LayoutConfig> configClass) {
        this.key = key;
        this.configClass = configClass;
    }

    @JsonCreator
    public static LayoutExtension fromString(String key) {
        for (var layout : values()) {
            if (layout.key.equalsIgnoreCase(key)) {
                return layout;
            }
        }

        throw new IllegalArgumentException("Unknown layout extension key: " + key);
    }

    @JsonValue
    public String getKey() {
        return key;
    }

    public Class<? extends LayoutConfig> getConfigClass() {
        return configClass;
    }

    @Override
    public String toString() {
        return key;
    }

}
