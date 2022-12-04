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

package edu.wisc.library.ocfl.core.extension.storage.layout;

import edu.wisc.library.ocfl.core.extension.OcflExtension;
import edu.wisc.library.ocfl.core.extension.OcflExtensionConfig;

/**
 * Interface for OCFL storage layout extensions. Storage layout extensions are expected to be used as singletons.
 * They are dynamically loaded when needed, and MUST have a no-arg constructor. The extension is configured by a call
 * to its {@link #init} method.
 */
public interface OcflStorageLayoutExtension extends OcflExtension {

    /**
     * Configures the extension. This method must be called before the extension can be used.
     *
     * @param config extension configuration
     */
    void init(OcflExtensionConfig config);

    /**
     * The class that represents the extensions deserialized configuration.
     *
     * @return configuration class
     */
    Class<? extends OcflExtensionConfig> getExtensionConfigClass();

    /**
     * Maps an object id to its path within the OCFL storage root.
     *
     * @param objectId the object id
     * @return the path to the object root relative to the OCFL storage root
     */
    String mapObjectId(String objectId);

    /**
     * @return the description text that should be used in ocfl_layout.json
     */
    String getDescription();
}
