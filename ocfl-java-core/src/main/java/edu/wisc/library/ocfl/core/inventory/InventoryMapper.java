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

import at.favre.lib.bytes.Bytes;
import com.fasterxml.jackson.databind.InjectableValues;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.exception.CorruptObjectException;
import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionNum;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.util.ObjectMappers;
import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.security.DigestInputStream;
import java.util.Collection;

/**
 * Wrapper around Jackson's ObjectMapper for serializing and deserializing Inventories. The ObjectMapper setup is a finicky
 * and I do not recommend changing it unless you are sure you know what you're doing.
 */
public class InventoryMapper {

    private final ObjectMapper objectMapper;
    private final ContentPathConstraintProcessor contentPathConstraints;

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
        this.contentPathConstraints = ContentPathConstraints.minimal();
    }

    public void write(Path destination, Inventory inventory) {
        try {
            objectMapper.writeValue(destination.toFile(), inventory);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    public void write(OutputStream outputStream, Inventory inventory) {
        try {
            objectMapper.writeValue(outputStream, inventory);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    public Inventory read(String objectRootPath, DigestAlgorithm digestAlgorithm, Path path) {
        return readInternal(false, null, objectRootPath, digestAlgorithm, path);
    }

    public Inventory read(String objectRootPath, DigestAlgorithm digestAlgorithm, InputStream inputStream) {
        return readInternal(false, null, objectRootPath, digestAlgorithm, inputStream);
    }

    public Inventory readMutableHead(
            String objectRootPath, RevisionNum revisionNum, DigestAlgorithm digestAlgorithm, Path path) {
        return readInternal(true, revisionNum, objectRootPath, digestAlgorithm, path);
    }

    public Inventory readMutableHead(
            String objectRootPath, RevisionNum revisionNum, DigestAlgorithm digestAlgorithm, InputStream inputStream) {
        return readInternal(true, revisionNum, objectRootPath, digestAlgorithm, inputStream);
    }

    public Inventory readNoDigest(String objectRootPath, Path path) {
        return readInternal(false, null, objectRootPath, null, path);
    }

    public Inventory readNoDigest(String objectRootPath, InputStream inputStream) {
        return readInternal(false, null, objectRootPath, null, inputStream);
    }

    public Inventory readMutableHeadNoDigest(String objectRootPath, RevisionNum revisionNum, Path path) {
        return readInternal(true, revisionNum, objectRootPath, null, path);
    }

    public Inventory readMutableHeadNoDigest(String objectRootPath, RevisionNum revisionNum, InputStream inputStream) {
        return readInternal(true, revisionNum, objectRootPath, null, inputStream);
    }

    private Inventory readInternal(
            boolean mutableHead,
            RevisionNum revisionNum,
            String objectRootPath,
            DigestAlgorithm digestAlgorithm,
            Path path) {
        return readInternal(mutableHead, revisionNum, objectRootPath, readBytes(path, digestAlgorithm));
    }

    private Inventory readInternal(
            boolean mutableHead,
            RevisionNum revisionNum,
            String objectRootPath,
            DigestAlgorithm digestAlgorithm,
            InputStream inputStream) {
        return readInternal(mutableHead, revisionNum, objectRootPath, readBytes(inputStream, digestAlgorithm));
    }

    private Inventory readInternal(
            boolean mutableHead, RevisionNum revisionNum, String objectRootPath, ReadResult readResult) {
        try {
            Inventory inventory = objectMapper
                    .reader(new InjectableValues.Std()
                            .addValue("revisionNum", revisionNum)
                            .addValue("mutableHead", mutableHead)
                            .addValue("objectRootPath", objectRootPath)
                            .addValue("inventoryDigest", readResult.digest))
                    .forType(Inventory.class)
                    .readValue(readResult.bytes);

            // Ensure that all content paths are valid to avoid security problems due to malicious inventories
            inventory.getManifest().values().stream()
                    .flatMap(Collection::stream)
                    .forEach(contentPathConstraints::apply);

            return inventory;
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private ReadResult readBytes(Path path, DigestAlgorithm digestAlgorithm) {
        try (var stream = Files.newInputStream(path)) {
            return readBytes(stream, digestAlgorithm);
        } catch (NoSuchFileException e) {
            throw new CorruptObjectException(String.format("Inventory missing at: %s", path), e);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private ReadResult readBytes(InputStream stream, DigestAlgorithm digestAlgorithm) {
        var bufferedStream = new BufferedInputStream(stream);

        try {
            byte[] bytes;
            String digest = null;

            if (digestAlgorithm != null) {
                var wrapped = new DigestInputStream(bufferedStream, digestAlgorithm.getMessageDigest());
                bytes = wrapped.readAllBytes();
                digest = Bytes.wrap(wrapped.getMessageDigest().digest()).encodeHex();
            } else {
                bytes = bufferedStream.readAllBytes();
            }

            return new ReadResult(bytes, digest);
        } catch (IOException e) {
            throw new OcflIOException(e);
        }
    }

    private static class ReadResult {
        final byte[] bytes;
        final String digest;

        public ReadResult(byte[] bytes, String digest) {
            this.bytes = bytes;
            this.digest = digest;
        }
    }
}
