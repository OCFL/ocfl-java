package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;

public class BitSetPathCharConstraintTest {

    @Test
    public void shouldRejectBlocklistedChar() {
        var constraint = BitSetPathCharConstraint.blockList('a', '\u0000');

        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('a', "path");
        });
        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('\u0000', "path");
        });
        constraint.apply('b', "path");
    }

    @Test
    public void shouldRejectRangeOfBlocklistedChars() {
        var constraint = BitSetPathCharConstraint.blockListRange((char) 0, (char) 31);

        var cr = new AtomicReference<Character>();

        for (char c = 0; c < 32; c++) {
            cr.set(c);
            assertThrows(PathConstraintException.class, () -> {
                constraint.apply(cr.get(), "path");
            });
        }
        constraint.apply(' ', "path");
    }

    @Test
    public void shouldRejectCharNotOnAcceptList() {
        var constraint = BitSetPathCharConstraint.acceptList('b', 'c');

        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('a', "path");
        });
        constraint.apply('b', "path");
    }

    @Test
    public void shouldOnlyAcceptCharsInAcceptListedRange() {
        var constraint = BitSetPathCharConstraint.acceptListRange('a', 'z');

        var cr = new AtomicReference<Character>();

        for (char c = 'a'; c <= 'z'; c++) {
            cr.set(c);
            constraint.apply(cr.get(), "path");
        }

        assertThrows(PathConstraintException.class, () -> {
            constraint.apply(' ', "path");
        });
    }
}
