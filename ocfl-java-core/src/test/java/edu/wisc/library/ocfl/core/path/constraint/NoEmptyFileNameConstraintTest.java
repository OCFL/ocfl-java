package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;

public class NoEmptyFileNameConstraintTest {

    @Test
    public void shouldRejectEmptyFileNames() {
        assertThrows(PathConstraintException.class, () -> {
            new NonEmptyFileNameConstraint().apply("", "//");
        });
    }

    @Test
    public void shouldAcceptPathsWithNonEmptyFileNames() {
        new NonEmptyFileNameConstraint().apply("path", "path");
    }
}
