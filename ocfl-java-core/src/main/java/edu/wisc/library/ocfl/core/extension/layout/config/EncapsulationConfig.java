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

package edu.wisc.library.ocfl.core.extension.layout.config;

import java.util.Objects;

/**
 * Describes how objects should be encapsulated when using an n-tuple storage layout
 */
public class EncapsulationConfig {

    private String defaultString;
    private EncapsulationType type;
    private EncodingType encoding;
    private Integer substringSize;

    /**
     * The encapsulation string to use when the encoded id is to short to be used for encapsulation. This value must be
     * longer than the size of the n-tuples.
     *
     * @return default encapsulation string
     */
    public String getDefaultString() {
        return defaultString;
    }

    /**
     * The encapsulation string to use when the encoded id is to short to be used for encapsulation. This value must be
     * longer than the size of the n-tuples.
     *
     * @param defaultString  default encapsulation string
     */
    public EncapsulationConfig setDefaultString(String defaultString) {
        this.defaultString = defaultString;
        return this;
    }

    /**
     * The type of encapsulation to use
     *
     * @return type
     */
    public EncapsulationType getType() {
        return type;
    }

    /**
     * The type of encapsulation to use
     *
     * @param type type
     */
    public EncapsulationConfig setType(EncapsulationType type) {
        this.type = type;
        return this;
    }

    /**
     * The encoding to use on the object id before using it as the encapsulation directory. This can be the same or different
     * from the encoding that was used on the object id to generate the n-tuple path.
     *
     * <p>This should only be set when {@link EncapsulationType#ID} is used.
     *
     * @return encoding
     */
    public EncodingType getEncoding() {
        return encoding;
    }

    /**
     * The encoding to use on the object id before using it as the encapsulation directory. This can be the same or different
     * from the encoding that was used on the object id to generate the n-tuple path.
     *
     * <p>This should only be set when {@link EncapsulationType#ID} is used.
     *
     * @param encoding encoding
     */
    public EncapsulationConfig setEncoding(EncodingType encoding) {
        this.encoding = encoding;
        return this;
    }

    /**
     * The number of characters from the end of the encoded id to use as the encapsulation directory.
     *
     * <p>This should only be set when {@link EncapsulationType#SUBSTRING} is used.
     *
     * @return substring size
     */
    public Integer getSubstringSize() {
        return substringSize;
    }

    /**
     * The number of characters from the end of the encoded id to use as the encapsulation directory.
     *
     * <p>This should only be set when {@link EncapsulationType#SUBSTRING} is used.
     *
     * @param substringSize  substring size
     */
    public EncapsulationConfig setSubstringSize(Integer substringSize) {
        this.substringSize = substringSize;
        return this;
    }

    @Override
    public String toString() {
        return "EncapsulationConfig{" +
                "defaultString='" + defaultString + '\'' +
                ", type=" + type +
                ", encoding=" + encoding +
                ", substringSize=" + substringSize +
                '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        EncapsulationConfig that = (EncapsulationConfig) o;
        return Objects.equals(defaultString, that.defaultString) &&
                type == that.type &&
                encoding == that.encoding &&
                Objects.equals(substringSize, that.substringSize);
    }

    @Override
    public int hashCode() {
        return Objects.hash(defaultString, type, encoding, substringSize);
    }

}
