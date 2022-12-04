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

package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.util.Enforce;
import java.nio.file.FileSystems;
import java.util.regex.Pattern;

/**
 * This class enforces constraints on content paths. At the minimum, content paths must adhere to the following constraints:
 *
 * <ul>
 *     <li>Cannot have a leading OR trailing /</li>
 *     <li>Cannot contain the following filenames: '.', '..'</li>
 *     <li>Cannot contain an empty filename</li>
 *     <li>Windows only: Cannot contain a \</li>
 * </ul>
 *
 * <p>Additional custom constraints may be applied as needed.
 */
public class DefaultContentPathConstraintProcessor implements ContentPathConstraintProcessor {

    private static final PathConstraint LEADING_SLASH_CONSTRAINT = BeginEndPathConstraint.mustNotBeginWith("/");
    private static final PathConstraint TRAILING_SLASH_CONSTRAINT = BeginEndPathConstraint.mustNotEndWith("/");
    private static final FileNameConstraint DOT_CONSTRAINT =
            RegexPathConstraint.mustNotContain(Pattern.compile("^\\.{1,2}$"));
    private static final FileNameConstraint NON_EMPTY_CONSTRAINT = new NonEmptyFileNameConstraint();
    private static final PathCharConstraint BACKSLASH_CONSTRAINT = new BackslashPathSeparatorConstraint();

    private final PathConstraintProcessor storagePathConstraintProcessor;
    private final PathConstraintProcessor contentPathConstraintProcessor;

    /**
     * Use this to construct a ContentPathConstraintProcessor
     *
     * @return builder
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Use this to construct a ContentPathConstraintProcessor
     */
    public static class Builder {

        private PathConstraintProcessor storagePathConstraintProcessor;
        private PathConstraintProcessor contentPathConstraintProcessor;

        public Builder() {
            storagePathConstraintProcessor = PathConstraintProcessor.builder().build();
            contentPathConstraintProcessor = PathConstraintProcessor.builder().build();
        }

        /**
         * Sets the PathConstraintProcessor to apply to storage paths.
         *
         * @param storagePathConstraintProcessor storage path constraint processor
         * @return builder
         */
        public Builder storagePathConstraintProcessor(PathConstraintProcessor storagePathConstraintProcessor) {
            this.storagePathConstraintProcessor =
                    Enforce.notNull(storagePathConstraintProcessor, "storagePathConstraintProcessor cannot be null");
            return this;
        }

        /**
         * Sets the PathConstraintProcessor to apply to content paths
         *
         * @param contentPathConstraintProcessor content path constraint processor
         * @return builder
         */
        public Builder contentPathConstraintProcessor(PathConstraintProcessor contentPathConstraintProcessor) {
            this.contentPathConstraintProcessor =
                    Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
            return this;
        }

        /**
         * @return new ContentPathConstraintProcessor
         */
        public DefaultContentPathConstraintProcessor build() {
            return new DefaultContentPathConstraintProcessor(
                    storagePathConstraintProcessor, contentPathConstraintProcessor);
        }
    }

    /**
     * Constructs a new ContentPathConstraintProcessor. Alternatively, use {@link DefaultContentPathConstraintProcessor.Builder}.
     *
     * @param storagePathConstraintProcessor the constraints to apply to storage paths
     * @param contentPathConstraintProcessor the constraints to apply to content paths
     */
    public DefaultContentPathConstraintProcessor(
            PathConstraintProcessor storagePathConstraintProcessor,
            PathConstraintProcessor contentPathConstraintProcessor) {
        this.storagePathConstraintProcessor =
                Enforce.notNull(storagePathConstraintProcessor, "storagePathConstraintProcessor cannot be null");
        this.contentPathConstraintProcessor =
                Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");

        if (filesystemUsesBackslashSeparator()) {
            this.contentPathConstraintProcessor.prependCharConstraint(BACKSLASH_CONSTRAINT);
        }

        // Add the required content path constraints to the beginning of the content path constraint processor
        // constraint list
        this.contentPathConstraintProcessor
                .prependFileNameConstraint(DOT_CONSTRAINT)
                .prependFileNameConstraint(NON_EMPTY_CONSTRAINT)
                .prependPathConstraint(TRAILING_SLASH_CONSTRAINT)
                .prependPathConstraint(LEADING_SLASH_CONSTRAINT);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String contentPath, String storagePath) {
        storagePathConstraintProcessor.apply(storagePath);
        contentPathConstraintProcessor.apply(contentPath);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(String contentPath) {
        contentPathConstraintProcessor.apply(contentPath);
    }

    private boolean filesystemUsesBackslashSeparator() {
        // TODO note: this not 100% accurate because the filesystem that the repository is on may be different than the
        //            default filesystem
        var pathSeparator = FileSystems.getDefault().getSeparator().charAt(0);
        return pathSeparator == '\\';
    }
}
