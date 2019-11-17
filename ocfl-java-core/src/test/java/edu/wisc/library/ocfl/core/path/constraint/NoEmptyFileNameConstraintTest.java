package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertThrows;

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
