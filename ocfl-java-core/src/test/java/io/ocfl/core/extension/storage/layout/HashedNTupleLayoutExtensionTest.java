/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019-2021 University of Wisconsin Board of Regents
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

package io.ocfl.core.extension.storage.layout;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.ocfl.api.exception.OcflExtensionException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleLayoutConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HashedNTupleLayoutExtensionTest {

    private String objectId = "http://library.wisc.edu/123";

    private HashedNTupleLayoutExtension ext;

    @BeforeEach
    public void setup() {
        ext = new HashedNTupleLayoutExtension();
    }

    @Test
    public void shouldMapIdWithDefaultConfig() {
        ext.init(new HashedNTupleLayoutConfig());
        assertMapping("ed7/558/5a6/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithLargeTuples() {
        ext.init(new HashedNTupleLayoutConfig().setTupleSize(6).setShortObjectRoot(true));
        assertMapping("ed7558/5a6e8d/eef3b3/f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithLargeTuplesAndMoreTuples() {
        ext.init(new HashedNTupleLayoutConfig()
                .setTupleSize(6)
                .setNumberOfTuples(6)
                .setShortObjectRoot(true));
        assertMapping("ed7558/5a6e8d/eef3b3/f620e5/f6d099/9908c0/9e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithShortRoot() {
        ext.init(new HashedNTupleLayoutConfig().setShortObjectRoot(true));
        assertMapping("ed7/558/5a6/e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithFlatConfig() {
        ext.init(new HashedNTupleLayoutConfig().setTupleSize(0).setNumberOfTuples(0));
        assertMapping("ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    @Test
    public void shouldMapIdWithDifferentAlgorithm() {
        ext.init(new HashedNTupleLayoutConfig().setDigestAlgorithm(DigestAlgorithm.md5));
        assertMapping("609/ea7/968/609ea7968f37d4d61aa0e0df7458f6b1");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeAndNumTuplesNotBoth0() {
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleLayoutConfig().setTupleSize(0));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("both must be 0");
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleLayoutConfig().setNumberOfTuples(0));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("both must be 0");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeTimesNumTuplesGreaterThanTotalChars() {
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleLayoutConfig().setTupleSize(5).setNumberOfTuples(13));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("sha256 digests only have 64 characters");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeTimesNumTuplesEqualTotalCharsAndShortRootTrue() {
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleLayoutConfig()
                            .setTupleSize(4)
                            .setNumberOfTuples(16)
                            .setShortObjectRoot(true));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("shortObjectRoot cannot be set to true");
    }

    @Test
    public void shouldMapIdWhenEqualTotalChars() {
        ext.init(new HashedNTupleLayoutConfig().setTupleSize(4).setNumberOfTuples(16));
        assertMapping(
                "ed75/585a/6e8d/eef3/b3f6/20e5/f6d0/9999/08c0/9e92/cad5/e112/aa5e/ac55/0700/0d8b/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b");
    }

    private void assertMapping(String expectedMapping) {
        var result = ext.mapObjectId(objectId);
        assertEquals(expectedMapping, result);
    }
}
