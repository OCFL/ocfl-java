package edu.wisc.library.ocfl.core.util;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PercentEscaperTest {

    @Test
    public void shouldOnlyEncodeCharsInSet() {
        var escaper = PercentEscaper.builder()
                .addUnsafeChars("~`! )ѾB")
                .build();

        assertEquals("3%20ja%7e%25(%42%296j%d1%be1%218Ѯ0%60", escaper.escape("3 ja~%(B)6jѾ1!8Ѯ0`"));
    }

    @Test
    public void shouldOnlyEncodeCharsInRange() {
        var escaper = PercentEscaper.builder()
                .addUnsafeCharRange((char)0, (char)31)
                .build();

        var builder = new StringBuilder();
        for (char c = 0; c < 32; c++) {
            builder.append(c);
        }
        builder.append("blah!123۞");

        assertEquals("%00%01%02%03%04%05%06%07%08%09%0a%0b%0c%0d%0e%0f%10%11%12%13%14%15%16%17%18%19%1a%1b%1c%1d%1e%1fblah!123۞",
                escaper.escape(builder.toString()));
    }

    @Test
    public void shouldNotEncodeCharsInSet() {
        var escaper = PercentEscaper.builderWithSafeAlphaNumeric()
                .addSafeChars("~`! )Ѿ")
                .build();

        assertEquals("3 ja~%25%28B)6jѾ1!8%d1%ae0`", escaper.escape("3 ja~%(B)6jѾ1!8Ѯ0`"));
    }



    @Test
    public void shouldNotEncodeCharsInRange() {
        var escaper = PercentEscaper.builderWithSafeAlphaNumeric()
                .addSafeCharRange((char)32, (char)40)
                .build();

        var builder = new StringBuilder();
        for (char c = 0; c < 32; c++) {
            builder.append(c);
        }
        builder.append("!$%asd۞");

        assertEquals("%00%01%02%03%04%05%06%07%08%09%0a%0b%0c%0d%0e%0f%10%11%12%13%14%15%16%17%18%19%1a%1b%1c%1d%1e%1f!$%25asd%db%9e",
                escaper.escape(builder.toString()));
    }

    @Test
    public void cannotSetUnsafeCharsWhenSafeAlreadySet() {
        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builderWithSafeAlphaNumeric().addUnsafeChars("a"));

        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builderWithSafeAlphaNumeric().addUnsafeCharRange('a', 'z'));

        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builder().addSafeChars("a").addUnsafeChars("a"));

        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builder().addSafeChars("a").addUnsafeCharRange('a', 'z'));
    }

    @Test
    public void cannotSetSafeCharsWhenUnsafeAlreadySet() {
        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builder().addUnsafeChars("a").addSafeChars("a"));

        assertThrows(IllegalArgumentException.class,
                () -> PercentEscaper.builder().addUnsafeChars("a").addSafeCharRange('a', 'z'));
    }

}
