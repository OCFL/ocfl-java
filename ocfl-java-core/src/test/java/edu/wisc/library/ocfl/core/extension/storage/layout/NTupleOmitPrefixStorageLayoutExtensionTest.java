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
package edu.wisc.library.ocfl.core.extension.storage.layout;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import edu.wisc.library.ocfl.api.exception.OcflInputException;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;

/**
 * @author vcrema
 * @since 2021-10-25
 */
public class NTupleOmitPrefixStorageLayoutExtensionTest {

    private NTupleOmitPrefixStorageLayoutExtension ext;
    private NTupleOmitPrefixStorageLayoutConfig config;

    @BeforeEach
    public void setUp() {
        ext = new NTupleOmitPrefixStorageLayoutExtension();
        config = new NTupleOmitPrefixStorageLayoutConfig();
    }

    @Test
    public void testNullDelimiter() {
        assertThrows(OcflInputException.class, () -> config.setDelimiter(null),
                "Expected OcflInputException");
    }

    @Test
    public void testEmptyDelimiter() {
        assertThrows(OcflInputException.class, () -> config.setDelimiter(""),
                "Expected OcflInputException");
    }
    
    @Test
    public void testNegativeTupleSize() {
        assertThrows(OcflInputException.class, () -> config.setTupleSize(-1),
                "Expected OcflInputException");
    }

    @Test
    public void testZeroTupleSize() {
        assertThrows(OcflInputException.class, () -> config.setTupleSize(0),
                "Expected OcflInputException");
    }
    
    @Test
    public void testNegativeNumberOfTuples() {
        assertThrows(OcflInputException.class, () -> config.setNumberOfTuples(-1),
                "Expected OcflInputException");
    }

    @Test
    public void testZeroNumberOfTuples() {
        assertThrows(OcflInputException.class, () -> config.setNumberOfTuples(0),
                "Expected OcflInputException");
    }
    
    @Test
    public void testNonAsciiId() {
        config.setDelimiter("/");
        ext.init(config);
        assertThrows(OcflExtensionException.class, () -> ext.mapObjectId("jå∫∆a/vµa2bl√øog"),
                "Expected OcflExtensionException");
    }


    @Test
    public void testDelimiterNotFound() {
        config.setDelimiter("/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        assertThrows(OcflExtensionException.class, () -> ext.mapObjectId("namespace:12887296"),
                "Expected OcflExtensionException");
    }

    @Test
    public void testOneOccurrenceOfSingleCharDelimiter() {
        config.setDelimiter(":");     
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        String result = ext.mapObjectId("namespace:128872961");
        assertEquals("128/872/961/128872961", result);
    }

    @Test
    public void testTwoOccurrencesOfSingleCharDelimiter() {
        config.setDelimiter(":");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        String result = ext.mapObjectId("urn:uuid:6e8bc430-9c3a-11d9-9669-0800200c9a66");
        assertEquals("6e8/bc4/30-/6e8bc430-9c3a-11d9-9669-0800200c9a66", result);
    }

    @Test
    public void testTwoOccurrencesOfSingleCharDelimiterDRS() {
        config.setDelimiter(":");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        String result = ext.mapObjectId("urn-3:HUL.DRS.OBJECT:128872961");
        assertEquals("128/872/961/128872961", result);
    }

    @Test
    public void testOneOccurrenceOfMultiCharDelimiter() {
        config.setDelimiter("edu/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false
        
        String result = ext.mapObjectId("https://institution.edu/344879388");
        assertEquals("344/879/388/344879388", result);
    }

    @Test
    public void testOneOccurrenceOfMultiCharDelimiterCaseInsensitive() {
        config.setDelimiter("EDU/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        String result = ext.mapObjectId("https://institution.edu/344879388");
        assertEquals("344/879/388/344879388", result);
    }

    @Test
    public void testTwoOccurrencesOfMultiCharDelimiter() {
        config.setDelimiter("edu/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false
        
        String result = ext.mapObjectId("https://institution.edu/abc/edu/f8.05v");
        assertEquals("000/f8./05v/f8.05v", result);
    }
    
    @Test
    public void testTupleAndLeftPadding() {
        config.setDelimiter(":");
        config.setTupleSize(4);
        config.setNumberOfTuples(2);
        ext.init(config);
        //Defaults:
        //zeroPadding: "left",
        //reverseObjectRoot: false

        String result = ext.mapObjectId("namespace:1288729");
        assertEquals("0128/8729/1288729", result);
    }
    
    @Test
    public void testTupleAndRightPadding() {
        config.setDelimiter(":");
        config.setTupleSize(4);
        config.setNumberOfTuples(2);
        config.setZeroPadding(NTupleOmitPrefixStorageLayoutConfig.ZeroPadding.RIGHT);
        ext.init(config);
        //Defaults:
        //reverseObjectRoot: false

        String result = ext.mapObjectId("namespace:1288729");
        assertEquals("1288/7290/1288729", result);
    }
    
    @Test
    public void testReverseAndRightPadding() {
        config.setDelimiter(":");
        config.setTupleSize(4);
        config.setNumberOfTuples(2);
        config.setReverseObjectRoot(true);
        config.setZeroPadding(NTupleOmitPrefixStorageLayoutConfig.ZeroPadding.RIGHT);
        ext.init(config);
        
        String result = ext.mapObjectId("namespace:1288729");
        assertEquals("9278/8210/1288729", result);
    }
    
    @Test
    public void testOneOccurrenceOfMultiCharDelimiterAtEnd() {
        config.setDelimiter("edu/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false

        assertThrows(OcflExtensionException.class, () -> ext.mapObjectId("https://institution.edu/"),
                "Expected OcflExtensionException");
    }
    
    @Test
    public void testMultiCharDelimiterNotFound() {
        config.setDelimiter("com/");
        ext.init(config);
        //Defaults:
        //tupleSize: 3
        //numberOfTuples: 3,
        //zeroPadding: "left",
        //reverseObjectRoot: false
        
        assertThrows(OcflExtensionException.class, () -> ext.mapObjectId("https://institution.edu/344879388"),
                "Expected OcflExtensionException");
    }
}
