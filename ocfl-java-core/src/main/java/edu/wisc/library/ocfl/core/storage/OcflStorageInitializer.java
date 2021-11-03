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

package edu.wisc.library.ocfl.core.storage;

import edu.wisc.library.ocfl.api.model.OcflVersion;
import edu.wisc.library.ocfl.core.extension.ExtensionSupportEvaluator;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;
import edu.wisc.library.ocfl.core.extension.storage.layout.OcflStorageLayoutExtension;

/**
 * Initializes an OCFL repository by either creating a new repository root or reading the configuration from an
 * existing repository.
 */
public interface OcflStorageInitializer {

    /**
     * Initializes a new OCFL storage root when the storage root does not already exist. If the storage root does exist,
     * then the configuration is examined to ensure that it matches what was configured programmatically. If nothing
     * was configured programmatically, then the configuration on disk is used without validation.
     *
     * @param ocflVersion OCFL version the repository conforms to
     * @param layoutConfig storage layout configuration, may be null when not creating a new repository
     * @param supportEvaluator repository extension evaluator
     * @return the storage layout extension the repository uses
     */
    OcflStorageLayoutExtension initializeStorage(OcflVersion ocflVersion,
                                                 OcflExtensionConfig layoutConfig,
                                                 ExtensionSupportEvaluator supportEvaluator);
}
