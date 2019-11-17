package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.util.regex.Pattern;

/**
 * This class enforces constraints on content paths. At the minimum, content paths must adhere to the following constraints:
 *
 * <ul>
 *     <li>Cannot have a trailing /</li>
 *     <li>Cannot contain the following filenames: '.', '..'</li>
 *     <li>Cannot contain an empty filename</li>
 *     <li>Windows only: Cannot contain a \</li>
 * </ul>
 *
 * <p>Additional custom constraints may be applied as needed.
 */
public class ContentPathConstraintProcessor {

    private static final PathConstraint TRAILING_SLASH_CONSTRAINT = RegexPathConstraint
            .mustNotContain(Pattern.compile("^.*/$"));
    private static final FileNameConstraint DOT_CONSTRAINT = RegexPathConstraint
            .mustNotContain(Pattern.compile("^\\.{1,2}$"));
    private static final FileNameConstraint NON_EMPTY_CONSTRAINT = new NonEmptyFileNameConstraint();
    private static final PathCharConstraint BACKSLASH_CONSTRAINT = new BackslashPathSeparatorConstraint();

    private PathConstraintProcessor storagePathConstraintProcessor;
    private PathConstraintProcessor contentPathConstraintProcessor;

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

        public Builder storagePathConstraintProcessor(PathConstraintProcessor storagePathConstraintProcessor) {
            this.storagePathConstraintProcessor = Enforce.notNull(storagePathConstraintProcessor, "storagePathConstraintProcessor cannot be null");
            return this;
        }

        public Builder contentPathConstraintProcessor(PathConstraintProcessor contentPathConstraintProcessor) {
            this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
            return this;
        }

        public ContentPathConstraintProcessor build() {

            return new ContentPathConstraintProcessor(storagePathConstraintProcessor, contentPathConstraintProcessor);
        }

    }

    public ContentPathConstraintProcessor(PathConstraintProcessor storagePathConstraintProcessor, PathConstraintProcessor contentPathConstraintProcessor) {
        this.storagePathConstraintProcessor = Enforce.notNull(storagePathConstraintProcessor, "storagePathConstraintProcessor cannot be null");
        this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");

        this.contentPathConstraintProcessor
                .prependPathConstraint(TRAILING_SLASH_CONSTRAINT)
                .prependFileNameConstraint(NON_EMPTY_CONSTRAINT)
                .prependFileNameConstraint(DOT_CONSTRAINT)
                .prependCharConstraint(BACKSLASH_CONSTRAINT);
    }

    /**
     * Applies the configured path constrains to the content path and storage path. If any constraints fail, a {@link edu.wisc.library.ocfl.api.exception.PathConstraintException}
     * is thrown.
     *
     * @param contentPath the content path relative a version's content directory
     * @param storagePath the content path relative the OCFL repository root
     * @throws edu.wisc.library.ocfl.api.exception.PathConstraintException when a constraint fails
     */
    public void apply(String contentPath, String storagePath) {
        storagePathConstraintProcessor.apply(storagePath);
        contentPathConstraintProcessor.apply(contentPath);
    }

}
