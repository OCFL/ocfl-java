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

package io.ocfl.core.extension.storage.layout;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.ocfl.api.exception.OcflExtensionException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.core.extension.storage.layout.config.HashedNTupleIdEncapsulationLayoutConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class HashedNTupleIdEncapsulationLayoutExtensionTest {

    private String objectId = "http://library.wisc.edu/123";

    private HashedNTupleIdEncapsulationLayoutExtension ext;

    @BeforeEach
    public void setup() {
        ext = new HashedNTupleIdEncapsulationLayoutExtension();
    }

    @Test
    public void shouldMapIdWithDefaultConfig() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig());
        assertMapping("ed7/558/5a6/http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldMapIdWithLargeTuples() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setTupleSize(6));
        assertMapping("ed7558/5a6e8d/eef3b3/http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldMapIdWithLargeTuplesAndMoreTuples() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setTupleSize(6).setNumberOfTuples(6));
        assertMapping("ed7558/5a6e8d/eef3b3/f620e5/f6d099/9908c0/http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldMapIdWithFlatConfig() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setTupleSize(0).setNumberOfTuples(0));
        assertMapping("http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldMapIdWithDifferentAlgorithm() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setDigestAlgorithm(DigestAlgorithm.md5));
        assertMapping("609/ea7/968/http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeAndNumTuplesNotBoth0() {
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setTupleSize(0));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("both must be 0");
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setNumberOfTuples(0));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("both must be 0");
    }

    @Test
    public void shouldThrowExceptionWhenTupleSizeTimesNumTuplesGreaterThanTotalChars() {
        assertThatThrownBy(() -> {
                    ext.init(new HashedNTupleIdEncapsulationLayoutConfig()
                            .setTupleSize(5)
                            .setNumberOfTuples(13));
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("sha256 digests only have 64 characters");
    }

    @Test
    public void shouldMapIdWhenEqualTotalChars() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig().setTupleSize(4).setNumberOfTuples(16));
        assertMapping(
                "ed75/585a/6e8d/eef3/b3f6/20e5/f6d0/9999/08c0/9e92/cad5/e112/aa5e/ac55/0700/0d8b/http%3a%2f%2flibrary%2ewisc%2eedu%2f123");
    }

    @Test
    public void shouldNotEncodeCharactersWhenAllSafe() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig());
        assertMapping("object-123", "cdd/8a1/f4e/object-123");
    }

    @Test
    public void shouldEncodeCharactersWhenNotSafe() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig());
        assertMapping("..Hor/rib:lè-$id", "373/529/21a/%2e%2eHor%2frib%3al%c3%a8-%24id");
    }

    @Test
    public void shouldTruncateWhenEncodedIdLongerThan100Chars() {
        ext.init(new HashedNTupleIdEncapsulationLayoutConfig());
        assertMapping(
                "۵ݨݯژښڙڜڛڝڠڱݰݣݫۯ۞ۆݰ",
                "72d/744/ab2/%db%b5%dd%a8%dd%af%da%98%da%9a%da%99%da%9c%da%9b%da%9d%da%a0%da%b1%dd%b0%dd%a3%dd%ab%db%af%db%9e%db%-72d744ab28e696afd14423026efe0ca8954e8f1b3fd21e86f06e89375b4de005");
    }

    private void assertMapping(String expectedMapping) {
        assertMapping(objectId, expectedMapping);
    }

    private void assertMapping(String objectId, String expectedMapping) {
        var result = ext.mapObjectId(objectId);
        assertEquals(expectedMapping, result);
    }
}
