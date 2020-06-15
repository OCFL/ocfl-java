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

package edu.wisc.library.ocfl.core.path;

import edu.wisc.library.ocfl.api.model.VersionId;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.OcflConstants;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionId;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.path.sanitize.NoOpPathSanitizer;
import edu.wisc.library.ocfl.core.path.sanitize.PathSanitizer;
import edu.wisc.library.ocfl.core.util.FileUtil;


/**
 * This class sanitizes logical paths, applies content path constraints, and finally returns a mapped content path.
 * It should only be used on input logical paths.
 */
public class ContentPathMapper {

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
            contentPathConstraintProcessor = ContentPathConstraints.none();
        }

        public Builder pathSanitizer(PathSanitizer pathSanitizer) {
            this.pathSanitizer = Enforce.notNull(pathSanitizer, "pathSanitizer cannot be null");
            return this;
        }

        public Builder contentPathConstraintProcessor(ContentPathConstraintProcessor contentPathConstraintProcessor) {
            this.contentPathConstraintProcessor = Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
            return this;
        }

        public ContentPathMapper buildStandardVersion(Inventory inventory) {
            Enforce.notNull(inventory, "inventory cannot be null");
            return new ContentPathMapper(pathSanitizer, contentPathConstraintProcessor,
                    inventory.getObjectRootPath(), inventory.resolveContentDirectory(),
                    Enforce.notNull(inventory.nextVersionId(), "versionId cannot be null"), null);
        }

        public ContentPathMapper buildMutableVersion(Inventory inventory) {
            Enforce.notNull(inventory, "inventory cannot be null");
            return new ContentPathMapper(pathSanitizer, contentPathConstraintProcessor,
                    inventory.getObjectRootPath(), inventory.resolveContentDirectory(),
                    null, Enforce.notNull(inventory.nextRevisionId(), "revisionId cannot be null"));
        }

    }

    public ContentPathMapper(PathSanitizer pathSanitizer, ContentPathConstraintProcessor contentPathConstraintProcessor,
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
