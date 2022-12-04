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

import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.api.model.FileChange;
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import java.util.Map;
import java.util.Objects;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class FileChangeMatcher extends TypeSafeMatcher<FileChange> {

    private FileChangeType changeType;
    private ObjectVersionId objectVersionId;
    private String filePath;
    private String storagePath;
    private Map<DigestAlgorithm, String> fixity;

    private VersionInfoMatcher versionInfoMatcher;

    FileChangeMatcher(
            FileChangeType changeType,
            ObjectVersionId objectVersionId,
            String filePath,
            String storagePath,
            VersionInfoMatcher versionInfoMatcher,
            Map<DigestAlgorithm, String> fixity) {
        this.changeType = changeType;
        this.objectVersionId = objectVersionId;
        this.filePath = filePath;
        this.storagePath = storagePath;
        this.versionInfoMatcher = versionInfoMatcher;
        this.fixity = fixity;
    }

    @Override
    protected boolean matchesSafely(FileChange item) {
        return Objects.equals(filePath, item.getPath())
                && Objects.equals(changeType, item.getChangeType())
                && Objects.equals(objectVersionId, item.getObjectVersionId())
                && Objects.equals(storagePath, item.getStorageRelativePath())
                && versionInfoMatcher.matches(item.getVersionInfo())
                && Objects.equals(fixity, item.getFixity());
    }

    @Override
    public void describeTo(Description description) {
        description
                .appendText("FileChange{filePath=")
                .appendValue(filePath)
                .appendText(", storagePath=")
                .appendValue(storagePath)
                .appendText(", changeType=")
                .appendValue(changeType)
                .appendText(", objectVersionId=")
                .appendValue(objectVersionId)
                .appendText(", fixity=")
                .appendValue(fixity)
                .appendText("}");
    }
}
