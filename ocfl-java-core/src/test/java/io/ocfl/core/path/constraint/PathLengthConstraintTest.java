package io.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import io.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;

public class PathLengthConstraintTest {

    @Test
    public void shouldRejectPathsWithTooManyCharacters() {
        assertThrows(PathConstraintException.class, () -> {
            PathLengthConstraint.maxChars(10).apply("12345678901", "path");
        });
        assertThrows(PathConstraintException.class, () -> {
            PathLengthConstraint.maxChars(10).apply("12345678901");
        });
    }

    @Test
    public void shouldRejectPathsWithTooManyBytes() {
        assertThrows(PathConstraintException.class, () -> {
            PathLengthConstraint.maxBytes(10).apply("¬¬¬¬¬¬", "path");
        });
        assertThrows(PathConstraintException.class, () -> {
            PathLengthConstraint.maxBytes(10).apply("¬¬¬¬¬¬");
        });
    }

    @Test
    public void shouldAcceptPathsThatDoNotHaveTooManyChars() {
        PathLengthConstraint.maxChars(10).apply("1234567890", "path");
        PathLengthConstraint.maxChars(10).apply("1234567890");
    }

    @Test
    public void shouldAcceptPathsThatDoNotHaveTooManyBytes() {
        PathLengthConstraint.maxBytes(10).apply("¬¬¬¬¬", "path");
        PathLengthConstraint.maxBytes(10).apply("¬¬¬¬¬");
    }
}
