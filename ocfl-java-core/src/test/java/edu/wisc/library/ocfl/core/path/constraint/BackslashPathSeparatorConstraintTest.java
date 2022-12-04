package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledOnOs;
import org.junit.jupiter.api.condition.OS;

public class BackslashPathSeparatorConstraintTest {

    @Test
    @EnabledOnOs(OS.LINUX)
    public void shouldAllowBackslashesOnLinux() {
        new BackslashPathSeparatorConstraint().apply('\\', "path\\with\\backslashes");
    }

    @Test
    @EnabledOnOs(OS.WINDOWS)
    public void shouldNotAllowBackslashesOnWindows() {
        assertThrows(PathConstraintException.class, () -> {
            new BackslashPathSeparatorConstraint().apply('\\', "path\\with\\backslashes");
        });
    }
}
