package edu.wisc.library.ocfl.core.extension.layout;

import edu.wisc.library.ocfl.core.extension.layout.config.FlatLayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.LayoutConfig;
import edu.wisc.library.ocfl.core.extension.layout.config.NTupleLayoutConfig;

/**
 * Representation of the OCFL ocfl_layout.json file
 */
public class LayoutSpec {

    private static final LayoutSpec FLAT = new LayoutSpec()
            .setKey(LayoutExtension.FLAT)
            .setDescription("Flat layout");

    private static final LayoutSpec N_TUPLE = new LayoutSpec()
            .setKey(LayoutExtension.N_TUPLE)
            .setDescription("n-tuple layout");

    // TODO how are versions handled?
    private LayoutExtension key;
    private String description;

    public static LayoutSpec layoutSpecForConfig(LayoutConfig config) {
        if (config instanceof FlatLayoutConfig) {
            return FLAT;
        } else if (config instanceof NTupleLayoutConfig) {
            return N_TUPLE;
        } else {
            throw new IllegalArgumentException("Unknown layout config: " + config);
        }
    }

    public LayoutExtension getKey() {
        return key;
    }

    public LayoutSpec setKey(LayoutExtension key) {
        this.key = key;
        return this;
    }

    public String getDescription() {
        return description;
    }

    public LayoutSpec setDescription(String description) {
        this.description = description;
        return this;
    }

    @Override
    public String toString() {
        return "LayoutSpec{" +
                "key=" + key +
                ", description='" + description + '\'' +
                '}';
    }

}
