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

package edu.wisc.library.ocfl.core.extension;

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.util.Enforce;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Set;

/**
 * Evaluates if an extension is supported and acts accordingly if it is not.
 */
public class ExtensionSupportEvaluator {

    private static final Logger LOG = LoggerFactory.getLogger(ExtensionSupportEvaluator.class);

    private static final Set<String> BUILTIN_EXTS = Set.of(
            OcflConstants.MUTABLE_HEAD_EXT_NAME,
            OcflConstants.DIGEST_ALGORITHMS_EXT_NAME
    );

    private final UnsupportedExtensionBehavior behavior;
    private final Set<String> ignore;

    public ExtensionSupportEvaluator() {
        this(UnsupportedExtensionBehavior.FAIL, Collections.emptySet());
    }

    public ExtensionSupportEvaluator(UnsupportedExtensionBehavior behavior, Set<String> ignore) {
        this.behavior = Enforce.notNull(behavior, "behavior cannot be null");
        this.ignore = Enforce.notNull(ignore, "ignore cannot be null");
    }

    /**
     * Checks if an extension is supported by ocfl-java, and returns true if it is. If it is not, either an exception
     * is thrown or a WARNing is logged based on how this class is configured.
     *
     * @param extensionName the name of the extension to check
     * @return true if the extension is supported
     */
    public boolean checkSupport(String extensionName) {
        if (!(OcflExtensionRegistry.isSupported(extensionName)
                || BUILTIN_EXTS.contains(extensionName))) {
            var ignored = ignore.contains(extensionName);
            var message = String.format("Extension %s is not currently supported by ocfl-java.", extensionName);

            if (behavior == UnsupportedExtensionBehavior.FAIL) {
                if (!ignored) {
                    throw new OcflExtensionException(message);
                } else {
                    LOG.warn(message);
                    return false;
                }
            } else if (!ignored) {
                LOG.warn(message);
                return false;
            }
        }

        return true;
    }

}
