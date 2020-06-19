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

import edu.wisc.library.ocfl.api.OcflObjectVersion;
import edu.wisc.library.ocfl.api.model.VersionId;
import org.hamcrest.Description;
import org.hamcrest.Matchers;
import org.hamcrest.TypeSafeMatcher;

import java.util.Arrays;
import java.util.Collection;
import java.util.Objects;

public class OcflObjectVersionMatcher extends TypeSafeMatcher<OcflObjectVersion> {

    private String objectId;
    private VersionId versionId;
    private VersionInfoMatcher versionInfoMatcher;
    private Collection<OcflObjectVersionFileMatcher> fileMatchers;

    OcflObjectVersionMatcher(String objectId, String versionId, VersionInfoMatcher versionInfoMatcher, OcflObjectVersionFileMatcher... fileMatchers) {
        this.objectId = objectId;
        this.versionId = VersionId.fromString(versionId);
        this.versionInfoMatcher = versionInfoMatcher;
        this.fileMatchers = Arrays.asList(fileMatchers);
    }

    @Override
    protected boolean matchesSafely(OcflObjectVersion item) {
        return Objects.equals(objectId, item.getObjectId())
                && Objects.equals(versionId, item.getVersionId())
                && versionInfoMatcher.matches(item.getVersionInfo())
                // Hamcrest has some infuriating overloaded methods...
                && Matchers.containsInAnyOrder((Collection) fileMatchers).matches(item.getFiles());
    }

    @Override
    public void describeTo(Description description) {
        description.appendText("VersionDetails{objectId=")
                .appendValue(objectId)
                .appendText(", versionId=")
                .appendValue(versionId)
                .appendText(", versionInfo=")
                .appendDescriptionOf(versionInfoMatcher)
                .appendText(", file=")
                .appendList("[", ",", "]", fileMatchers)
                .appendText("}");
    }

}
