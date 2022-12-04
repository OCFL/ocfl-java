package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class DefaultContentPathConstraintProcessorTest {

    @Test
    public void shouldEnforceBaselineConstraintsWhenNoOthersProvided() {
        assertThrows(PathConstraintException.class, () -> {
            DefaultContentPathConstraintProcessor.builder().build().apply("path/", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            DefaultContentPathConstraintProcessor.builder().build().apply("path/../file", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            DefaultContentPathConstraintProcessor.builder().build().apply("./file", "storage");
        });
        assertThrows(PathConstraintException.class, () -> {
            DefaultContentPathConstraintProcessor.builder().build().apply("path//file", "storage");
        });
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    public void shouldEnforceNoBackslashOnWindows() {
        assertThrows(PathConstraintException.class, () -> {
            DefaultContentPathConstraintProcessor.builder().build().apply("path\\file", "storage");
        });
    }

    @Test
    @EnabledOnOs(OS.LINUX)
    public void shouldNotEnforceNoBackslashOnLinux() {
        DefaultContentPathConstraintProcessor.builder().build().apply("path\\file", "storage");
    }

    @Test
    public void shouldApplyConstraintToStoragePath() {
        var processor = DefaultContentPathConstraintProcessor.builder()
                .storagePathConstraintProcessor(PathConstraintProcessor.builder()
                        .pathConstraint(PathLengthConstraint.maxChars(5))
                        .build())
                .build();

        assertThrows(PathConstraintException.class, () -> {
            processor.apply("cp", "storagePath");
        });
    }

    @Test
    public void shouldApplyConstraintToContentPath() {
        var processor = DefaultContentPathConstraintProcessor.builder()
                .contentPathConstraintProcessor(PathConstraintProcessor.builder()
                        .pathConstraint(PathLengthConstraint.maxChars(5))
                        .build())
                .build();

        assertThrows(PathConstraintException.class, () -> {
            processor.apply("contentPath", "sp");
        });
    }
}
