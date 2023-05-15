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

package io.ocfl.core.extension.storage.layout;

import io.ocfl.api.OcflConstants;
import io.ocfl.api.exception.OcflExtensionException;
import io.ocfl.core.extension.OcflExtensionConfig;
import io.ocfl.core.extension.storage.layout.config.FlatLayoutConfig;
import java.nio.file.FileSystems;

/**
 * Implementation of the Flat Storage Layout extension.
 *
 * @see <a href="https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html">https://ocfl.github.io/extensions/0002-flat-direct-storage-layout.html</a>
 */
public class FlatLayoutExtension implements OcflStorageLayoutExtension {

    public static final String EXTENSION_NAME = "0002-flat-direct-storage-layout";

    private final char pathSeparator;

    public FlatLayoutExtension() {
        pathSeparator = FileSystems.getDefault().getSeparator().charAt(0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String getExtensionName() {
        return EXTENSION_NAME;
    }

    @Override
    public String getDescription() {
        return "OCFL object identifiers are mapped directly to directory names"
                + " that are direct children of the OCFL storage root.";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public synchronized void init(OcflExtensionConfig config) {
        // NOOP this extension does not have any configuration
    }

    @Override
    public Class<? extends OcflExtensionConfig> getExtensionConfigClass() {
        return FlatLayoutConfig.class;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String mapObjectId(String objectId) {
        if (objectId.indexOf(pathSeparator) != -1) {
            throw new OcflExtensionException(String.format(
                    "The object id <%s> is incompatible with layout extension "
                            + "%s because it contains the path separator character.",
                    objectId, EXTENSION_NAME));
        } else if (OcflConstants.EXTENSIONS_DIR.equals(objectId)) {
            throw new OcflExtensionException(String.format(
                    "The object id <%s> is incompatible with layout extension "
                            + "%s because it conflicts with the extensions directory.",
                    objectId, EXTENSION_NAME));
        }

        return objectId;
    }
}
