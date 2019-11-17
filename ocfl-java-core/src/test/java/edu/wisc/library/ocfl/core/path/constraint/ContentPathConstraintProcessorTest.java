package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class ContentPathConstraintProcessorTest {

    @Test
    public void shouldEnforceBaselineConstraintsWhenNoOthersProvided() {
        assertThrows(PathConstraintException.class, () -> {
            ContentPathConstraintProcessor.builder().build().apply("path/", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            ContentPathConstraintProcessor.builder().build().apply("path/../file", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            ContentPathConstraintProcessor.builder().build().apply("./file", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            ContentPathConstraintProcessor.builder().build().apply("path//file", "storage");
        });
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    public void shouldEnforceNoBackslashOnWindows() {
        assertThrows(PathConstraintException.class, () -> {
            ContentPathConstraintProcessor.builder().build().apply("path\\file", "storage");
        });
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void shouldNotEnforceNoBackslashOnLinux() {
        ContentPathConstraintProcessor.builder().build().apply("path\\file", "storage");
    }

    @Test
    public void shouldApplyConstraintToStoragePath() {
        var processor = ContentPathConstraintProcessor.builder()
                .storagePathConstraintProcessor(PathConstraintProcessor.builder()
                        .pathConstraint(PathLengthConstraint.maxChars(5)).build()).build();

        assertThrows(PathConstraintException.class, () -> {
            processor.apply("cp", "storagePath");
        });
    }

    @Test
    public void shouldApplyConstraintToContentPath() {
        var processor = ContentPathConstraintProcessor.builder()
                .contentPathConstraintProcessor(PathConstraintProcessor.builder()
                        .pathConstraint(PathLengthConstraint.maxChars(5)).build()).build();

        assertThrows(PathConstraintException.class, () -> {
            processor.apply("contentPath", "sp");
        });
    }

}
