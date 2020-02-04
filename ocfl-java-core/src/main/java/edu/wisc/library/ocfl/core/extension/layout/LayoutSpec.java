/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

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
