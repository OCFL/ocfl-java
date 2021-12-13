/**
   Copyright @2021 President and Fellows of Harvard College

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
 */
package edu.wisc.library.ocfl.core.extension.storage.layout;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.core.extension.storage.layout.NTupleOmitPrefixStorageLayoutExtension;
import edu.wisc.library.ocfl.core.extension.storage.layout.config.NTupleOmitPrefixStorageLayoutConfig;

/**
 * @author vcrema
 * @since 2021-10-25
 */
public class NTupleOmitPrefixStorageExtensionTest {

    private NTupleOmitPrefixStorageLayoutExtension ext;
    private NTupleOmitPrefixStorageLayoutConfig config;

    @BeforeEach
    public void setUp() {
        ext = new NTupleOmitPrefixStorageLayoutExtension();
        config = new NTupleOmitPrefixStorageLayoutConfig();
    }

    @Test
    public void testNullDelimiter() {
    	assertThrows(IllegalArgumentException.class, () -> config.setDelimiter(null),
                "Expected IllegalArgumentException");
    }

    @Test
    public void testEmptyDelimiter() {
    	assertThrows(IllegalArgumentException.class, () -> config.setDelimiter(""),
                "Expected IllegalArgumentException");
    }
    
    @Test
    public void testNegativeTupleSize() {
    	assertThrows(IllegalArgumentException.class, () -> config.setTupleSize(-1),
                "Expected IllegalArgumentException");
    }

    @Test
    public void testZeroTupleSize() {
    	assertThrows(IllegalArgumentException.class, () -> config.setTupleSize(0),
                "Expected IllegalArgumentException");
    }
    
    @Test
    public void testNegativeNumberOfTuples() {
    	assertThrows(IllegalArgumentException.class, () -> config.setNumberOfTuples(-1),
                "Expected IllegalArgumentException");
    }

    @Test
    public void testZeroNumberOfTuples() {
        assertThrows(IllegalArgumentException.class, () -> config.setNumberOfTuples(0),
                "Expected IllegalArgumentException");
    }
    
    @Test
    public void testIncorrectZeroPadding() {
        assertThrows(IllegalArgumentException.class, () -> config.setZeroPadding("some value"),
                "Expected IllegalArgumentException");
    }
    
    @Test
    public void testZeroPaddingNoneAndShortString() {
        config.setDelimiter(":");
        config.setTupleSize(4);
        config.setNumberOfTuples(2);
        config.setZeroPadding(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_NONE);
        ext.init(config);
        //Defaults:
        //reverseObjectRoot: false

        //String is too short for requested tuples so it will throw runtime exception
        assertThrows(RuntimeException.class, () -> ext.mapObjectId("namespace:1288729"),
                "Expected RuntimeException");
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

        assertThrows(RuntimeException.class, () -> ext.mapObjectId("namespace:12887296"),
                "Expected RuntimeException");
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
        config.setZeroPadding(NTupleOmitPrefixStorageLayoutConfig.ZERO_PADDING_RIGHT);
        ext.init(config);
        //Defaults:
        //reverseObjectRoot: false

        String result = ext.mapObjectId("namespace:1288729");
        assertEquals("1288/7290/1288729", result);
    }
    
    @Test
    public void testReverseAndLeftPadding() {
        config.setDelimiter(":");
        config.setTupleSize(4);
        config.setNumberOfTuples(2);
        config.setReverseObjectRoot(true);
        config.setZeroPadding("right");
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

        assertThrows(RuntimeException.class, () -> ext.mapObjectId("https://institution.edu/"),
                "Expected RuntimeException");
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
        
        assertThrows(RuntimeException.class, () -> ext.mapObjectId("https://institution.edu/344879388"),
                "Expected RuntimeException");
    }
}
