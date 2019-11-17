package edu.wisc.library.ocfl.core.path;

import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.DefaultContentPathConstraints;
import edu.wisc.library.ocfl.core.path.sanitize.NoOpPathSanitizer;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.util.FileUtil;


/**
 * This class sanitizes logical paths, applies content path constraints, and finally returns a mapped content path.
 * It should only be used on input logical paths.
 */
public class ContentPathBuilder {

    private String objectRootPath;
    private String contentDirectory;
    private VersionId versionId;
    private RevisionId revisionId;

    private PathSanitizer pathSanitizer;
    private ContentPathConstraintProcessor contentPathConstraintProcessor;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private PathSanitizer pathSanitizer;
        private ContentPathConstraintProcessor contentPathConstraintProcessor;

        public Builder() {
            pathSanitizer = new NoOpPathSanitizer();
            contentPathConstraintProcessor = DefaultContentPathConstraints.none();
        }

        public Builder pathSanitizer(PathSanitizer pathSanitizer) {
            this.pathSanitizer = Enforce.notNull(pathSanitizer, "pathSanitizer cannot be null");
            return this;
        }

        public Builder contentPathConstraintProcessor(ContentPathConstraintProcessor contentPathConstraintProcessor) {
            this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
            return this;
        }

        public ContentPathBuilder buildStandardVersion(String objectRootPath, String contentDirectory, VersionId versionId) {
            return new ContentPathBuilder(pathSanitizer, contentPathConstraintProcessor,
                    objectRootPath, contentDirectory, Enforce.notNull(versionId, "versionId cannot be null"), null);
        }

        public ContentPathBuilder buildMutableVersion(String objectRootPath, String contentDirectory, RevisionId revisionId) {
            return new ContentPathBuilder(pathSanitizer, contentPathConstraintProcessor,
                    objectRootPath, contentDirectory, null, Enforce.notNull(revisionId, "revisionId cannot be null"));
        }

    }

    public ContentPathBuilder(PathSanitizer pathSanitizer, ContentPathConstraintProcessor contentPathConstraintProcessor,
                              String objectRootPath, String contentDirectory, VersionId versionId, RevisionId revisionId) {
        this.pathSanitizer = Enforce.notNull(pathSanitizer, "pathSanitizer cannot be null");
        this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be blank");
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
        this.versionId = versionId;
        this.revisionId = revisionId;
    }

    /**
     * Returns a content path for the given logical path after sanitizing it and ensuring that it meets the configured
     * content path constraints.
     *
     * @param logicalPath the logical path of the file in the object
     * @return the content path to store the file at
     */
    public String fromLogicalPath(String logicalPath) {
        var sanitizedLogicalPath = pathSanitizer.sanitize(logicalPath);

        var contentPath = contentPath(sanitizedLogicalPath);
        var storagePath = FileUtil.pathJoinFailEmpty(objectRootPath, contentPath);

        contentPathConstraintProcessor.apply(sanitizedLogicalPath, storagePath);

        return contentPath;
    }

    private String contentPath(String logicalPath) {
        if (isMutableHead()) {
            return FileUtil.pathJoinFailEmpty(
                    OcflConstants.MUTABLE_HEAD_VERSION_PATH,
                    contentDirectory,
                    revisionId.toString(),
                    logicalPath);
        }

        return FileUtil.pathJoinFailEmpty(
                versionId.toString(),
                contentDirectory,
                logicalPath);
    }

    private boolean isMutableHead() {
        return revisionId != null;
    }

}
