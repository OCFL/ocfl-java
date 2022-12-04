package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class RegexPathConstraintTest {

    @Test
    public void shouldRejectPathsThatMatchTheMustNotContainPattern() {
        assertThrows(PathConstraintException.class, () -> {
            RegexPathConstraint.mustNotContain(Pattern.compile("^\\.$")).apply(".");
        });
        assertThrows(PathConstraintException.class, () -> {
            RegexPathConstraint.mustNotContain(Pattern.compile("^\\.$")).apply(".", "path");
        });
    }

    @Test
    public void shouldAcceptPathsThatMatchMustNotContainPatterns() {
        RegexPathConstraint.mustNotContain(Pattern.compile("^\\.$")).apply("..");
        RegexPathConstraint.mustNotContain(Pattern.compile("^\\.$")).apply("..", "path");
    }

    @Test
    public void shouldRejectPathsThatDoNotMatchTheMustContainPattern() {
        assertThrows(PathConstraintException.class, () -> {
            RegexPathConstraint.mustContain(Pattern.compile("^\\.$")).apply("asdf");
        });
        assertThrows(PathConstraintException.class, () -> {
            RegexPathConstraint.mustContain(Pattern.compile("^\\.$")).apply("asdf", "path");
        });
    }

    @Test
    public void shouldAcceptPathsThatDoNotMatchMustContainPatterns() {
        RegexPathConstraint.mustContain(Pattern.compile("^\\.$")).apply(".");
        RegexPathConstraint.mustContain(Pattern.compile("^\\.$")).apply(".", "path");
    }
}
