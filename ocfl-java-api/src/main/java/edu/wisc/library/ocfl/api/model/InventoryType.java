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

package edu.wisc.library.ocfl.api.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonValue;
import edu.wisc.library.ocfl.api.exception.OcflJavaException;

public enum InventoryType {
    OCFL_1_0("https://ocfl.io/1.0/spec/#inventory"),
    OCFL_1_1("https://ocfl.io/1.1/spec/#inventory");

    private final String id;

    InventoryType(String id) {
        this.id = id;
    }

    @JsonValue
    public String getId() {
        return id;
    }

    @JsonIgnore
    public OcflVersion getOcflVersion() {
        switch (this) {
            case OCFL_1_0:
                return OcflVersion.OCFL_1_0;
            case OCFL_1_1:
                return OcflVersion.OCFL_1_1;
            default:
                throw new OcflJavaException("Unmapped inventory type " + id);
        }
    }

    @JsonCreator
    public static InventoryType fromValue(String value) {
        for (var entry : values()) {
            if (entry.id.equalsIgnoreCase(value)) {
                return entry;
            }
        }

        throw new OcflJavaException("Unknown InventoryType: " + value);
    }
}
