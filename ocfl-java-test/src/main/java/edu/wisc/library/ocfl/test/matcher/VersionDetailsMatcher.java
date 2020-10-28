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

import edu.wisc.library.ocfl.api.model.FileDetails;
import edu.wisc.library.ocfl.api.model.VersionDetails;
import edu.wisc.library.ocfl.api.model.VersionNum;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class VersionDetailsMatcher extends TypeSafeMatcher<VersionDetails> {

    private final String objectId;
    private final VersionNum versionNum;
    private final VersionInfoMatcher versionInfoMatcher;
    private final Collection<Matcher<FileDetails>> fileDetailsMatchers;

    VersionDetailsMatcher(String objectId, String versionNum, VersionInfoMatcher versionInfoMatcher, FileDetailsMatcher... fileDetailsMatchers) {
        this.objectId = objectId;
        this.versionNum = VersionNum.fromString(versionNum);
        this.versionInfoMatcher = versionInfoMatcher;
        this.fileDetailsMatchers = Arrays.asList(fileDetailsMatchers);
    }

    @Override
    protected boolean matchesSafely(VersionDetails item) {
        return Objects.equals(objectId, item.getObjectId())
                && Objects.equals(versionNum, item.getVersionNum())
                && versionInfoMatcher.matches(item.getVersionInfo())
                // Hamcrest has some infuriating overloaded methods...
                && Matchers.containsInAnyOrder((Collection) fileDetailsMatchers).matches(item.getFiles());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("VersionDetails{objectId=")
                .appendValue(objectId)
                .appendText(", versionNum=")
                .appendValue(versionNum)
                .appendText(", versionInfo=")
                .appendDescriptionOf(versionInfoMatcher)
                .appendText(", fileDetails=")
                .appendList("[", ",", "]", fileDetailsMatchers)
                .appendText("}");
    }

}
