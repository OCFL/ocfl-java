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

import edu.wisc.library.ocfl.api.OcflConstants;
import edu.wisc.library.ocfl.api.model.VersionNum;
import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.model.Inventory;
import edu.wisc.library.ocfl.core.model.RevisionNum;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraintProcessor;
import edu.wisc.library.ocfl.core.path.constraint.ContentPathConstraints;
import edu.wisc.library.ocfl.core.path.mapper.DirectLogicalPathMapper;
import edu.wisc.library.ocfl.core.path.mapper.LogicalPathMapper;
import edu.wisc.library.ocfl.core.util.FileUtil;

/**
 * This maps logical paths to content paths and applies content path constraints.
 */
public class ContentPathMapper {

    private final String objectRootPath;
    private final String contentDirectory;
    private final VersionNum versionNum;
    private final RevisionNum revisionNum;

    private final LogicalPathMapper logicalPathMapper;
    private final ContentPathConstraintProcessor contentPathConstraintProcessor;

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private LogicalPathMapper logicalPathMapper;
        private ContentPathConstraintProcessor contentPathConstraintProcessor;

        public Builder() {
            logicalPathMapper = new DirectLogicalPathMapper();
            contentPathConstraintProcessor = ContentPathConstraints.minimal();
        }

        public Builder logicalPathMapper(LogicalPathMapper logicalPathMapper) {
            this.logicalPathMapper = Enforce.notNull(logicalPathMapper, "logicalPathMapper cannot be null");
            return this;
        }

        public Builder contentPathConstraintProcessor(ContentPathConstraintProcessor contentPathConstraintProcessor) {
            this.contentPathConstraintProcessor =
                    Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
            return this;
        }

        public ContentPathMapper buildStandardVersion(Inventory inventory) {
            Enforce.notNull(inventory, "inventory cannot be null");
            return new ContentPathMapper(
                    logicalPathMapper,
                    contentPathConstraintProcessor,
                    inventory.getObjectRootPath(),
                    inventory.resolveContentDirectory(),
                    Enforce.notNull(inventory.nextVersionNum(), "versionNum cannot be null"),
                    null);
        }

        public ContentPathMapper buildMutableVersion(Inventory inventory) {
            Enforce.notNull(inventory, "inventory cannot be null");
            return new ContentPathMapper(
                    logicalPathMapper,
                    contentPathConstraintProcessor,
                    inventory.getObjectRootPath(),
                    inventory.resolveContentDirectory(),
                    null,
                    Enforce.notNull(inventory.nextRevisionNum(), "revisionNum cannot be null"));
        }
    }

    public ContentPathMapper(
            LogicalPathMapper logicalPathMapper,
            ContentPathConstraintProcessor contentPathConstraintProcessor,
            String objectRootPath,
            String contentDirectory,
            VersionNum versionNum,
            RevisionNum revisionNum) {
        this.logicalPathMapper = Enforce.notNull(logicalPathMapper, "logicalPathMapper cannot be null");
        this.contentPathConstraintProcessor =
                Enforce.notNull(contentPathConstraintProcessor, "contentPathConstraintProcessor cannot be null");
        this.objectRootPath = Enforce.notBlank(objectRootPath, "objectRootPath cannot be blank");
        this.contentDirectory = Enforce.notBlank(contentDirectory, "contentDirectory cannot be blank");
        this.versionNum = versionNum;
        this.revisionNum = revisionNum;
    }

    /**
     * Returns a content path for the given logical path after sanitizing it and ensuring that it meets the configured
     * content path constraints.
     *
     * @param logicalPath the logical path of the file in the object
     * @return the content path to store the file at
     */
    public String fromLogicalPath(String logicalPath) {
        var contentPathPart = logicalPathMapper.toContentPathPart(logicalPath);

        var contentPath = contentPath(contentPathPart);
        var storagePath = FileUtil.pathJoinFailEmpty(objectRootPath, contentPath);

        contentPathConstraintProcessor.apply(contentPathPart, storagePath);

        return contentPath;
    }

    private String contentPath(String contentPathPart) {
        if (isMutableHead()) {
            return FileUtil.pathJoinFailEmpty(
                    OcflConstants.MUTABLE_HEAD_VERSION_PATH, contentDirectory, revisionNum.toString(), contentPathPart);
        }

        return FileUtil.pathJoinFailEmpty(versionNum.toString(), contentDirectory, contentPathPart);
    }

    private boolean isMutableHead() {
        return revisionNum != null;
    }
}
