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
import edu.wisc.library.ocfl.api.model.FileChangeType;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;
import edu.wisc.library.ocfl.api.model.User;

import java.util.Map;

public class OcflMatchers {

    private OcflMatchers() {

    }

    public static VersionInfoMatcher versionInfo(String name, String address, String message) {
        return new VersionInfoMatcher(name, address, message);
    }

    public static VersionInfoMatcher versionInfo(User user, String message) {
        return new VersionInfoMatcher(user.getName(), user.getAddress(), message);
    }

    public static FileDetailsMatcher fileDetails(String filePath, String storagePath, Map<DigestAlgorithm, String> fixity) {
        return new FileDetailsMatcher(filePath, storagePath, fixity);
    }

    public static VersionDetailsMatcher versionDetails(String objectId, String versionNum, VersionInfoMatcher versionInfoMatcher, FileDetailsMatcher... fileDetailsMatchers) {
        return new VersionDetailsMatcher(objectId, versionNum, versionInfoMatcher, fileDetailsMatchers);
    }

    public static OcflObjectVersionFileMatcher versionFile(String filePath, String storagePath, String content, Map<DigestAlgorithm, String> fixity) {
        return new OcflObjectVersionFileMatcher(filePath, storagePath, content, fixity);
    }

    public static OcflObjectVersionMatcher objectVersion(String objectId, String versionNum, VersionInfoMatcher versionInfoMatcher, OcflObjectVersionFileMatcher... fileMatchers) {
        return new OcflObjectVersionMatcher(objectId, versionNum, versionInfoMatcher, fileMatchers);
    }

    public static FileChangeMatcher fileChange(FileChangeType changeType, ObjectVersionId objectVersionId, String filePath,
                                               String storagePath, VersionInfoMatcher versionInfoMatcher, Map<DigestAlgorithm, String> fixity) {
        return new FileChangeMatcher(changeType, objectVersionId, filePath, storagePath, versionInfoMatcher, fixity);
    }

}
