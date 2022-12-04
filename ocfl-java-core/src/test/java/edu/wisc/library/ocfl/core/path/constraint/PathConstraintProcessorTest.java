package edu.wisc.library.ocfl.core.path.constraint;

import static org.junit.jupiter.api.Assertions.assertThrows;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import java.util.regex.Pattern;
import org.junit.jupiter.api.Test;

public class PathConstraintProcessorTest {

    @Test
    public void shouldApplyAllConstraintsWhenAllPass() {
        defaultProcessor().apply("path");
    }

    @Test
    public void shouldFailWhenOneConstraintFails() {
        assertThrows(PathConstraintException.class, () -> {
            defaultProcessor().apply("path12345678");
        });
        assertThrows(PathConstraintException.class, () -> {
            defaultProcessor().apply("..");
        });
        assertThrows(PathConstraintException.class, () -> {
            defaultProcessor().apply("pa\\th");
        });
    }

    @Test
    public void shouldDoNothingWhenHasNoConstraints() {
        PathConstraintProcessor.builder().build().apply("path");
    }

    private PathConstraintProcessor defaultProcessor() {
        return PathConstraintProcessor.builder()
                .pathConstraint(PathLengthConstraint.maxChars(10))
                .fileNameConstraint(RegexPathConstraint.mustNotContain(Pattern.compile("^\\.\\.$")))
                .charConstraint(BitSetPathCharConstraint.blockList('\\'))
                .build();
    }
}
