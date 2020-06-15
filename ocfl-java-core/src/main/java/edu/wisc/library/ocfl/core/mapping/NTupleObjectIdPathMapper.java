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
import edu.wisc.library.ocfl.core.util.FileUtil;

/**
 * Maps an object id to a path by first encoding the object id and then splitting the encoded id into segments that are
 * used to construct a path. This implementation can be configured to support pairtree mapping or hash based mapping. The
 * advantage of hash based mapping is that objects will be spread evenly across the storage hierarchy.
 */
public class NTupleObjectIdPathMapper implements ObjectIdPathMapper {

    private Encoder encoder;
    private Encapsulator encapsulator;
    private int size;
    private int depth;
    private String defaultEncapsulation;

    private PathConstraintProcessor pathConstraints;

    /**
     * @param encoder the encoder to use to encode the object id
     * @param encapsulator the encapsulator to use to generate the name of the directory to encapsulate the object in
     * @param size the number of characters in each path tuple
     * @param depth the number of path tuples to generate before writing the encapsulation directory
     * @param defaultEncapsulation the default encapsulation directory to use for short paths
     */
    public NTupleObjectIdPathMapper(Encoder encoder, Encapsulator encapsulator, int size, int depth, String defaultEncapsulation) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
        this.encapsulator = Enforce.notNull(encapsulator, "encapsulator cannot be null");
        this.size = Enforce.expressionTrue(size > 0, size, "size must be greater than 0");
        this.depth = Enforce.expressionTrue(depth >= 0, depth, "depth must be greater than or equal to 0");
        this.defaultEncapsulation = Enforce.notBlank(defaultEncapsulation, "defaultEncapsulation cannot be blank");
        Enforce.expressionTrue(defaultEncapsulation.length() > size, size, "defaultEncapsulation string must be longer than the segment size");

        // depth of 0 == unbound
        if (this.depth == 0) {
            this.depth = Integer.MAX_VALUE;
        }

        this.pathConstraints = LogicalPathConstraints.constraints();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String map(String objectId) {
        var encodedId = encoder.encode(objectId);
        String path;

        if (encodedId.length() == 0) {
            throw new IllegalStateException("Encoded object id cannot be empty");
        } else if (encodedId.length() <= size) {
            path = FileUtil.pathJoinFailEmpty(encodedId, defaultEncapsulation);
        } else {
            var pathBuilder = new StringBuilder();

            for (var i = 0; i < depth; i++) {
                var start = i * size;

                if (start >= encodedId.length()) {
                    break;
                }

                var end = Math.min(start + size, encodedId.length());
                pathBuilder.append(encodedId, start, end).append("/");
            }

            // TODO there's weirdness if the object id is <= the segment size...
            pathBuilder.append(encapsulator.encapsulate(objectId, encodedId));

            path = pathBuilder.toString();
        }

        return pathConstraints.apply(path);
    }

}
