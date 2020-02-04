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
