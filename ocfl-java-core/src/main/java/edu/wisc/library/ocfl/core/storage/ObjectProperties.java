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

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.OcflVersion;

public class ObjectProperties {

    private OcflVersion ocflVersion;
    private DigestAlgorithm digestAlgorithm;
    private boolean extensions;

    public ObjectProperties() {
    }

    public ObjectProperties(OcflVersion ocflVersion,
                            DigestAlgorithm digestAlgorithm,
                            boolean extensions) {
        this.ocflVersion = ocflVersion;
        this.digestAlgorithm = digestAlgorithm;
        this.extensions = extensions;
    }

    /**
     * @return the OCFL version the object conforms to; may be null
     */
    public OcflVersion getOcflVersion() {
        return ocflVersion;
    }

    public void setOcflVersion(OcflVersion ocflVersion) {
        this.ocflVersion = ocflVersion;
    }

    /**
     * @return the digest algorithm used in the object's sidecar; may be null
     */
    public DigestAlgorithm getDigestAlgorithm() {
        return digestAlgorithm;
    }

    public void setDigestAlgorithm(DigestAlgorithm digestAlgorithm) {
        this.digestAlgorithm = digestAlgorithm;
    }

    /**
     * @return true if the object has an extensions directory
     */
    public boolean hasExtensions() {
        return extensions;
    }

    public void setExtensions(boolean extensions) {
        this.extensions = extensions;
    }

    @Override
    public String toString() {
        return "ObjectProperties{" +
                "ocflVersion=" + ocflVersion +
                ", digestAlgorithm=" + digestAlgorithm +
                ", extensions=" + extensions +
                '}';
    }
}
