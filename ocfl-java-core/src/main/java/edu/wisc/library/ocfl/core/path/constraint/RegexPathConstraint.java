package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.regex.Pattern;

/**
 * Validates that a path or filename does or does not match a regex
 */
public class RegexPathConstraint implements PathConstraint, FileNameConstraint {

    private Pattern pattern;
    private boolean mustMatch;

    /**
     * Creates a new constraint that validates paths do not match the regex
     *
     * @param pattern the regex that paths must not match
     * @return constraint
     */
    public static RegexPathConstraint mustNotContain(Pattern pattern) {
        return new RegexPathConstraint(false, pattern);
    }

    /**
     * Creates a new constraint that validates paths do match the regex
     *
     * @param pattern the regex that paths must match
     * @return constraint
     */
    public static RegexPathConstraint mustContain(Pattern pattern) {
        return new RegexPathConstraint(true, pattern);
    }

    public RegexPathConstraint(boolean mustMatch, Pattern pattern) {
        this.mustMatch = mustMatch;
        this.pattern = Enforce.notNull(pattern, "pattern cannot be null");
    }

    @Override
    public void apply(String path) {
        if (pattern.matcher(path).matches()) {
            if (!mustMatch) {
                throw new PathConstraintException(String.format("The path contains an invalid sequence %s: %s", pattern, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(String.format("The path must contain a sequence %s but does not: %s", pattern, path));
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String fileName, String path) {
        if (pattern.matcher(fileName).matches()) {
            if (!mustMatch) {
                throw new PathConstraintException(String.format("The filename '%s' contains an invalid sequence %s: %s", fileName, pattern, path));
            }
        } else if (mustMatch) {
            throw new PathConstraintException(String.format("The filename '%s' must contain a sequence %s but does not: %s", fileName, pattern, path));
        }
    }

}
