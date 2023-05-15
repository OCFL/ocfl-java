package io.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;

public class ContentPathConstraintsTest {

    @Test
    public void shouldEnforceUnixConstraints() {
        var processor = ContentPathConstraints.unix();

        // leading slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("/path", "/path");
        });

        // trailing slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/", "/path");
        });

        // more than 255 bytes
        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah",
                    "");
        });

        // NUL
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/\u0000/blah", "");
        });
    }

    @Test
    public void shouldEnforceWindowsConstraints() {
        var processor = ContentPathConstraints.windows();

        // leading slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("/path", "/path");
        });

        // trailing slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/", "/path");
        });

        // more than 255 chars
        processor.apply(
                "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                        + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                        + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                        + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                        + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                        + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah",
                "");

        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah",
                    "");
        });

        // reserved word
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/aux", "");
        });
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("COM6", "");
        });
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("lPt2.exe", "");
        });

        // space at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub /file", "");
        });

        // period at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub./file", "");
        });

        // blacklist chars
        assertCharRangeNotAllowed(processor, (char) 0, (char) 31);
        assertCharsNotAllowed(processor, '<', '>', ':', '"', '\\', '|', '?', '*');
    }

    @Test
    public void shouldEnforceCloudConstraints() {
        var processor = ContentPathConstraints.cloud();

        // leading slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("/path", "/path");
        });

        // trailing slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/", "/path");
        });

        // storage path max 1024 bytes
        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path",
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah");
        });

        // file name max 254 chars
        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah",
                    "");
        });

        // space at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub /file", "");
        });

        // period at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub./file", "");
        });

        // blacklist chars
        assertCharRangeNotAllowed(processor, (char) 0, (char) 31);
        assertCharRangeNotAllowed(processor, (char) 127, (char) 160);
        assertCharsNotAllowed(processor, '\\', '#', '[', ']', '*', '?');
    }

    @Test
    public void shouldEnforceAllConstraints() {
        var processor = ContentPathConstraints.all();

        // leading slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("/path", "/path");
        });

        // trailing slash
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/", "/path");
        });

        // storage path max 1024 bytes
        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path",
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah");
        });

        // file name max 254 chars
        assertThrows(PathConstraintException.class, () -> {
            processor.apply(
                    "path/\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF"
                            + "\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF\u2EEF/blah",
                    "");
        });

        // reserved word
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/aux", "");
        });
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("COM6", "");
        });
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("lPt2.exe", "");
        });

        // space at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub /file", "");
        });

        // period at end of file
        assertThrows(PathConstraintException.class, () -> {
            processor.apply("path/sub./file", "");
        });

        // blacklist chars
        assertCharRangeNotAllowed(processor, (char) 0, (char) 31);
        assertCharRangeNotAllowed(processor, (char) 127, (char) 160);
        assertCharsNotAllowed(processor, '<', '>', ':', '"', '\\', '|', '?', '*', '#', '[', ']');
    }

    private void assertCharRangeNotAllowed(ContentPathConstraintProcessor processor, char start, char end) {
        for (var c = start; c <= end; c++) {
            var path = "path/s" + c + "b/file";
            assertThrows(
                    PathConstraintException.class,
                    () -> {
                        processor.apply(path, "");
                    },
                    path);
        }
    }

    private void assertCharsNotAllowed(ContentPathConstraintProcessor processor, char... chars) {
        for (var c : chars) {
            var path = "path/s" + c + "b/file";
            assertThrows(
                    PathConstraintException.class,
                    () -> {
                        processor.apply(path, "");
                    },
                    path);
        }
    }
}
