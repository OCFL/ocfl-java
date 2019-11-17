package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.BitSet;

/**
 * Constraint that applies restrictions on what characters are allowed in file names.
 */
public class BitSetPathCharConstraint implements PathCharConstraint {

    private BitSet charSet;
    private boolean blackList;

    /**
     * Creates a constraint that rejects the specified characters
     *
     * @param chars the characters to reject
     * @return constraint
     */
    public static BitSetPathCharConstraint blackList(char... chars) {
        return build(true, chars);
    }

    /**
     * Creates a constraint that rejects all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint blackListRange(char start, char end) {
        return build(true, start, end);
    }

    /**
     * Creates a constraint that only accepts the specified characters
     *
     * @param chars the characters to accept
     * @return constraint
     */
    public static BitSetPathCharConstraint whiteList(char... chars) {
        return build(false, chars);
    }

    /**
     * Creates a constraint that accepts all of the characters in the given range
     *
     * @param start beginning of range, inclusive
     * @param end end of range, inclusive
     * @return constraint
     */
    public static BitSetPathCharConstraint whiteListRange(char start, char end) {
        return build(false, start, end);
    }

    private static BitSetPathCharConstraint build(boolean blackList, char... chars) {
        var charSet = new BitSet(256);
        for (var c : chars) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blackList, charSet);
    }

    private static BitSetPathCharConstraint build(boolean blackList, char start, char end) {
        var charSet = new BitSet(256);
        for (var c = start; c <= end; c++) {
            charSet.set(c);
        }
        return new BitSetPathCharConstraint(blackList, charSet);
    }

    public BitSetPathCharConstraint(boolean blackList, BitSet charSet) {
        this.blackList = blackList;
        this.charSet = Enforce.notNull(charSet, "charSet cannot be null");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(char c, String path) {
        if (charSet.get(c)) {
            if (blackList) {
                throwException(c, path);
            }
        } else if (!blackList) {
            throwException(c, path);
        }
    }

    private void throwException(char c, String path) {
        throw new PathConstraintException(String.format("The path contains the illegal character '%s': %s", c, path));
    }

}
