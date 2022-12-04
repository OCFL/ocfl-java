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

import edu.wisc.library.ocfl.api.model.VersionInfo;
import java.util.Objects;
import org.hamcrest.Description;
import org.hamcrest.TypeSafeMatcher;

public class VersionInfoMatcher extends TypeSafeMatcher<VersionInfo> {

    private String name;
    private String address;
    private String message;

    VersionInfoMatcher(String name, String address, String message) {
        this.name = name;
        this.address = address;
        this.message = message;
    }

    @Override
    protected boolean matchesSafely(VersionInfo item) {
        var matches = Objects.equals(item.getMessage(), message);

        if (item.getUser() != null) {
            matches &= Objects.equals(item.getUser().getName(), name)
                    && Objects.equals(item.getUser().getAddress(), address);
        } else {
            matches &= name == null && address == null;
        }

        return matches;
    }

    @Override
    public void describeTo(Description description) {
        description
                .appendText("VersionInfo{message=")
                .appendValue(message)
                .appendText(", user={name=")
                .appendValue(name)
                .appendText(", address=")
                .appendValue(address)
                .appendText("}}");
    }
}
