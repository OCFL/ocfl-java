/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package edu.wisc.library.ocfl.core.model;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.Objects;
import java.util.regex.Pattern;

/**
 * Represents the revision id of a mutable HEAD in the form of rN. Zero-padding is not allowed.
 */
public class RevisionId implements Comparable<RevisionId> {

    public static final RevisionId R1 = new RevisionId(1);

    private static final Pattern VALID_REVISION = Pattern.compile("^r\\d+$");

    private final long revisionNumber;
    private final long maxRevision;
    private final String stringValue;

    public static boolean isRevisionId(String value) {
        return VALID_REVISION.matcher(value).matches();
    }

    public static RevisionId fromString(String value) {
        if (!VALID_REVISION.matcher(value).matches()) {
            throw new IllegalArgumentException("Invalid RevisionId: " + value);
        }

        var numPart = value.substring(1);
        return new RevisionId(Long.parseLong(numPart));
    }

    public RevisionId(long revisionNumber) {
        this.revisionNumber = Enforce.expressionTrue(revisionNumber > 0, revisionNumber, "revisionNumber must be greater than 0");
        stringValue = "r" + revisionNumber;
        maxRevision = Long.MAX_VALUE;
    }

    /**
     * @return a new revision id with an incremented revision number
     */
    public RevisionId nextRevisionId() {
        var nextVersionNum = revisionNumber + 1;
        if (nextVersionNum > maxRevision) {
            throw new IllegalStateException("Cannot increment revision number. Current revision " + toString() + " is the highest possible.");
        }
        return new RevisionId(nextVersionNum);
    }

    /**
     * @return a new revision id with a decremented revision number
     */
    public RevisionId previousRevisionId() {
        if (revisionNumber == 1) {
            throw new IllegalStateException("Cannot decrement revision number. Current revision " + toString() + " is the lowest possible.");
        }
        return new RevisionId(revisionNumber - 1);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RevisionId versionId = (RevisionId) o;
        return Objects.equals(stringValue, versionId.stringValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(stringValue);
    }

    @Override
    public String toString() {
        return stringValue;
    }

    @Override
    public int compareTo(RevisionId o) {
        return Long.compare(revisionNumber, o.revisionNumber);
    }

}
