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

package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.encode.Encoder;
import edu.wisc.library.ocfl.core.path.constraint.PathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.LogicalPathConstraints;

/**
 * Converts an object id into a path by simply encoding the id. This results in a large number of objects rooted in the
 * same directory, and should not be used outside of small repositories.
 */
public class FlatObjectIdPathMapper implements ObjectIdPathMapper {

    private Encoder encoder;
    private PathConstraintProcessor pathConstraints;

    public FlatObjectIdPathMapper(Encoder encoder) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
        this.pathConstraints = LogicalPathConstraints.constraints();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String map(String objectId) {
        return pathConstraints.apply(encoder.encode(objectId));
    }

}
