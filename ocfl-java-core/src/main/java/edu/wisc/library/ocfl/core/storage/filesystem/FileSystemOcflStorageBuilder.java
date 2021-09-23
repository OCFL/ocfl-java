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

package edu.wisc.library.ocfl.core.storage.filesystem;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.ObjectMappers;

import java.nio.file.Path;

/**
 * Builder for constructing FileSystemOcflStorage objects. It is configured with sensible defaults and can minimally be
 * used as {@code FileSystemOcflStorage.builder().repositoryRoot(repoRoot).build()}
 */
public class FileSystemOcflStorageBuilder {

    private Path repositoryRoot;
    private ObjectMapper objectMapper;
    private FileSystemOcflStorageInitializer initializer;

    public FileSystemOcflStorageBuilder() {
        objectMapper = ObjectMappers.prettyPrintMapper();

    }

    /**
     * Sets the path to the OCFL repository root directory. Required.
     *
     * @param repositoryRoot the path to the OCFL storage root
     * @return builder
     */
    public FileSystemOcflStorageBuilder repositoryRoot(Path repositoryRoot) {
        this.repositoryRoot = Enforce.notNull(repositoryRoot, "repositoryRoot cannot be null");
        return this;
    }

    /**
     * Overrides the default ObjectMapper that's used to serialize ocfl_layout.json
     *
     * @param objectMapper object mapper
     * @return builder
     */
    public FileSystemOcflStorageBuilder objectMapper(ObjectMapper objectMapper) {
        this.objectMapper = Enforce.notNull(objectMapper, "objectMapper cannot be null");
        return this;
    }

    /**
     * Overrides the default {@link FileSystemOcflStorageInitializer}. Normally, this does not need to be set.
     *
     * @param initializer the initializer
     * @return builder
     */
    public FileSystemOcflStorageBuilder initializer(FileSystemOcflStorageInitializer initializer) {
        this.initializer = initializer;
        return this;
    }

    /**
     * Builds a new FileSystemOcflStorage object
     *
     * @return file system storage
     */
    public FileSystemOcflStorage build() {
        var init = initializer;
        if (init == null) {
            init = new FileSystemOcflStorageInitializer(objectMapper);
        }

        return new FileSystemOcflStorage(repositoryRoot, init);
    }

}
