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

package edu.wisc.library.ocfl.core.extension.storage.layout.impl;

import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.extension.storage.layout.HashedTruncatedNTupleExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.HashedTruncatedNTupleConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static edu.wisc.library.ocfl.test.OcflAsserts.assertThrowsWithMessage;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashedTruncatedNTupleExtensionTest {

    private String objectId = "http://library.wisc.edu/123";

    private HashedTruncatedNTupleExtension ext;

    @BeforeEach
    public void setup() {
        ext = new HashedTruncatedNTupleExtension();
    }

    @Test
    public void shouldMapIdWithDefaultConfig() {
        ext.init(new HashedTruncatedNTupleConfig());
        assertMapping("ed7/558/5a6/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithLargeTuples() {
        ext.init(new HashedTruncatedNTupleConfig().setTupleSize(6).setShortObjectRoot(true));
        assertMapping("ed7558/5a6e8d/eef3b3/f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithLargeTuplesAndMoreTuples() {
        ext.init(new HashedTruncatedNTupleConfig().setTupleSize(6).setNumberOfTuples(6).setShortObjectRoot(true));
        assertMapping("ed7558/5a6e8d/eef3b3/f620e5/f6d099/9908c0/9e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithShortRoot() {
        ext.init(new HashedTruncatedNTupleConfig().setShortObjectRoot(true));
        assertMapping("ed7/558/5a6/e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithFlatConfig() {
        ext.init(new HashedTruncatedNTupleConfig().setTupleSize(0).setNumberOfTuples(0));
        assertMapping("ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithDifferentAlgorithm() {
        ext.init(new HashedTruncatedNTupleConfig().setDigestAlgorithm(DigestAlgorithm.md5));
        assertMapping("609/ea7/968/609ea7968f37d4d61aa0e0df7458f6b1");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeAndNumTuplesNotBoth0() {
        assertThrowsWithMessage(OcflExtensionException.class, "both must be 0", () -> {
            ext.init(new HashedTruncatedNTupleConfig().setTupleSize(0));
        });
        assertThrowsWithMessage(OcflExtensionException.class, "both must be 0", () -> {
            ext.init(new HashedTruncatedNTupleConfig().setNumberOfTuples(0));
        });
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeTimesNumTuplesGreaterThanTotalChars() {
        assertThrowsWithMessage(OcflExtensionException.class, "sha256 digests only have 64 characters", () -> {
            ext.init(new HashedTruncatedNTupleConfig().setTupleSize(5).setNumberOfTuples(13));
        });
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeTimesNumTuplesEqualTotalCharsAndShortRootTrue() {
        assertThrowsWithMessage(OcflExtensionException.class, "shortObjectRoot cannot be set to true", () -> {
            ext.init(new HashedTruncatedNTupleConfig().setTupleSize(4).setNumberOfTuples(16).setShortObjectRoot(true));
        });
    }

    @Test
    public void shouldMapIdWhenEqualTotalChars() {
        ext.init(new HashedTruncatedNTupleConfig().setTupleSize(4).setNumberOfTuples(16));
        assertMapping("ed75/585a/6e8d/eef3/b3f6/20e5/f6d0/9999/08c0/9e92/cad5/e112/aa5e/ac55/0700/0d8b/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    private void assertMapping(String expectedMapping) {
        var result = ext.mapObjectId(objectId);
        assertEquals(expectedMapping, result);
    }

}
