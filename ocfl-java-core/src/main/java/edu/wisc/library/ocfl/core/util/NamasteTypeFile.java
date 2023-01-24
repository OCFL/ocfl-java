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

package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.exception.OcflIOException;
import edu.wisc.library.ocfl.api.util.Enforce;
import java.io.IOException;
import java.nio.file.Path;

public class NamasteTypeFile {

    private static final String NAMASTE_PREFIX = "0=";

    private final String tagValue;

    public NamasteTypeFile(String tagValue) {
        this.tagValue = Enforce.notBlank(tagValue, "tagValue cannot be blank");
    }

    public String fileName() {
        return NAMASTE_PREFIX + tagValue;
    }

    public String fileContent() {
        return tagValue + "\n";
    }

    public void writeFile(Path directory) {
        try {
            FileUtil.writeString(directory.resolve(fileName()), fileContent());
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }
}
