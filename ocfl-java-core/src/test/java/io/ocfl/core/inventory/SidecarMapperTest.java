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

package io.ocfl.core.inventory;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.ocfl.api.exception.CorruptObjectException;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class SidecarMapperTest {

    @TempDir
    public Path temp;

    @Test
    public void parseSidecarWithSingleSpace() {
        var expected = "abc123";
        var actual = SidecarMapper.readDigestRequired(writeFile(expected + " "));
        assertEquals(expected, actual);
    }

    @Test
    public void parseSidecarWithMultipleSpace() {
        var expected = "abc123";
        var actual = SidecarMapper.readDigestRequired(writeFile(expected + "   "));
        assertEquals(expected, actual);
    }

    @Test
    public void parseSidecarWithSinglTab() {
        var expected = "abc123";
        var actual = SidecarMapper.readDigestRequired(writeFile(expected + "\t"));
        assertEquals(expected, actual);
    }

    @Test
    public void parseSidecarWithMultipleTabs() {
        var expected = "abc123";
        var actual = SidecarMapper.readDigestRequired(writeFile(expected + "\t\t"));
        assertEquals(expected, actual);
    }

    @Test
    public void failWhenSinglePart() {
        assertThrows(CorruptObjectException.class, () -> {
            SidecarMapper.readDigestRequired(writeFile("blah"));
        });
    }

    @Test
    public void failWhenTooManyParts() {
        assertThrows(CorruptObjectException.class, () -> {
            SidecarMapper.readDigestRequired(writeFile("blah blah blah "));
        });
    }

    private Path writeFile(String contents) {
        var file = temp.resolve("sidecar-" + Instant.now().toEpochMilli());
        try {
            Files.writeString(file, contents + "inventory.json", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file;
    }
}
