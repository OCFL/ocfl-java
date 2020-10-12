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

package edu.wisc.library.ocfl.core.inventory;

import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Path;

/**
 * Wrapper around Jackson's ObjectMapper for serializing and deserializing Inventories. The ObjectMapper setup is a finicky
 * and I do not recommend changing it unless you are sure you know what you're doing.
 */
public class InventoryMapper {

    private final ObjectMapper objectMapper;

    /**
     * Creates an InventoryMapper that will pretty print JSON files. This should be used when you value human readability
     * over disk space usage.
     *
     * @return mapper
     */
    public static InventoryMapper prettyPrintMapper() {
        return new InventoryMapper(ObjectMappers.prettyPrintMapper());
    }

    /**
     * Creates an InventoryMapper that creates JSON files with as little whitespace as possible. This should be used when
     * minimizing disk space usage is more important than human readability.
     *
     * @return mapper
     */
    public static InventoryMapper defaultMapper() {
        return new InventoryMapper(ObjectMappers.defaultMapper());
    }
    
    /**
     * Should use InventoryMapper.defaultMapper() or InventoryMapper.prettyPrintMapper() unless you know what you're doing.
     *
     * @param objectMapper object mapper
     */
    public InventoryMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
    }

    public void write(Path destination, Inventory inventory) {
        try {
            objectMapper.writeValue(destination.toFile(), inventory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void write(OutputStream outputStream, Inventory inventory) {
        try {
            objectMapper.writeValue(outputStream, inventory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public Inventory read(String objectRootPath, Path path) {
        return readInternal(false, null, objectRootPath, path);
    }

    public Inventory read(String objectRootPath, InputStream inputStream) {
        return readInternal(false, null, objectRootPath, inputStream);
    }

    public Inventory readMutableHead(String objectRootPath, RevisionId revisionId, Path path) {
        return readInternal(true, revisionId, objectRootPath, path);
    }

    public Inventory readMutableHead(String objectRootPath, RevisionId revisionId, InputStream inputStream) {
        return readInternal(true, revisionId, objectRootPath, inputStream);
    }

    private Inventory readInternal(boolean mutableHead, RevisionId revisionId, String objectRootPath, Path path) {
        try {
            return objectMapper.reader(
                    new InjectableValues.Std()
                            .addValue("revisionId", revisionId)
                            .addValue("mutableHead", mutableHead)
                            .addValue("objectRootPath", objectRootPath))
                    .forType(Inventory.class)
                    .readValue(path.toFile());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Inventory readInternal(boolean mutableHead, RevisionId revisionId, String objectRootPath, InputStream inputStream) {
        try {
            return objectMapper.reader(
                    new InjectableValues.Std()
                            .addValue("revisionId", revisionId)
                            .addValue("mutableHead", mutableHead)
                            .addValue("objectRootPath", objectRootPath))
                    .forType(Inventory.class)
                    .readValue(inputStream);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
