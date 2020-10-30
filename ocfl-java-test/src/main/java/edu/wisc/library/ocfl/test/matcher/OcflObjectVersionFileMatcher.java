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

package edu.wisc.library.ocfl.test.matcher;

import edu.wisc.library.ocfl.api.io.FixityCheckInputStream;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.OcflObjectVersionFile;
import edu.wisc.library.ocfl.test.TestHelper;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

import java.util.Map;
import java.util.Objects;

public class OcflObjectVersionFileMatcher extends TypeSafeMatcher<OcflObjectVersionFile> {

    private String filePath;
    private String storagePath;
    private String content;
    private Map<DigestAlgorithm, String> fixity;

    OcflObjectVersionFileMatcher(String filePath, String storagePath, String content, Map<DigestAlgorithm, String> fixity) {
        this.filePath = filePath;
        this.storagePath = storagePath;
        this.content = content;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(OcflObjectVersionFile item) {
        return Objects.equals(filePath, item.getPath())
                && Objects.equals(storagePath, item.getStorageRelativePath())
                && Objects.equals(fixity, item.getFixity())
                && sameContent(item.getStream());
    }

    private boolean sameContent(FixityCheckInputStream stream) {
        if (content != null) {
            var actual = TestHelper.inputToString(stream);
            stream.checkFixity();
            return Objects.equals(content, actual);
        }
        return true;
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("FileDetails{filePath=")
                .appendValue(filePath)
                .appendText(", storagePath=")
                .appendValue(storagePath)
                .appendValue(", content=")
                .appendValue(content)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");

    }

}
