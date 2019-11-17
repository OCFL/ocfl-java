package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import org.junit.jupiter.api.Test;

import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertThrows;

public class BitSetPathCharConstraintTest {

    @Test
    public void shouldRejectBlacklistedChar() {
        var constraint = BitSetPathCharConstraint.blackList('a', '\u0000');

        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('a', "path");
        });
        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('\u0000', "path");
        });
        constraint.apply('b', "path");
    }

    @Test
    public void shouldRejectRangeOfBlacklistedChars() {
        var constraint = BitSetPathCharConstraint.blackListRange((char) 0, (char) 31);

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
    public void shouldRejectCharNotOnWhiteList() {
        var constraint = BitSetPathCharConstraint.whiteList('b', 'c');

        assertThrows(PathConstraintException.class, () -> {
            constraint.apply('a', "path");
        });
        constraint.apply('b', "path");
    }

    @Test
    public void shouldOnlyAcceptCharsInWhiteListedRange() {
        var constraint = BitSetPathCharConstraint.whiteListRange('a', 'z');

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
