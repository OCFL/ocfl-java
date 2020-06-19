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

package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.VersionInfo;
import edu.wisc.library.ocfl.api.model.ObjectVersionId;

import java.util.function.Consumer;

/**
 * Defines APIs for implementing the OCFL Mutable HEAD Extension. These APIs are outside of the scope of the core OCFL
 * specification, and provide additional functionality for staging object content within the OCFL storage root before
 * committing it to a version. It is imperative to understand that staged content is NOT part of the core OCFL object,
 * and it CANNOT be interpreted by an OCFL client that does not implement the Mutable HEAD Extension.
 */
public interface MutableOcflRepository extends OcflRepository {

    /**
     * Stages changes to the mutable HEAD of the specified object. If the object does not already have a mutable HEAD,
     * a new one is created. Otherwise, the changes are applied on top of the existing mutable HEAD, without creating a new version.
     *
     * <p>The changes contained in the mutable HEAD are NOT part of the core OCFL object. Use {@link #commitStagedChanges}
     * to convert the mutable version into an immutable version that's part of the core OCFL object. This should be done
     * whenever possible.
     *
     * <p>If the current HEAD version of the object does not match the version specified in the request, the update will
     * be rejected. If the request specifies the HEAD version, then no version check will be preformed.
     *
     * @param objectId the id of the object. If set to a specific version, then the update will only occur
     *                 if this version matches the head object version in the repository.
     * @param versionInfo information about the changes to the object. Can be null.
     * @param objectUpdater code block within which updates to an object may be made
     * @return The objectId and version of the new object version
     * @throws NotFoundException when no object can be found for the specified objectId
     * @throws ObjectOutOfSyncException when the object was modified by another process before these changes could be committed
     */
    ObjectVersionId stageChanges(ObjectVersionId objectId, VersionInfo versionInfo, Consumer<OcflObjectUpdater> objectUpdater);

    /**
     * Converts the staged changes in the mutable HEAD into an immutable core OCFL version that can be read by any OCFL client.
     *
     * <p>This operation will fail if any object versions were created between the time changes were staged and
     * when they were committed. To resolve this problem, the staged changes must either be purged using {@link #purgeStagedChanges},
     * or the object must be manually edited to resolve the version conflict.
     *
     * <p>If the object does not have staged changes, then nothing happens.
     *
     * @param objectId the id of the object
     * @param versionInfo information about the changes to the object. Can be null.
     * @return The objectId and version of the committed version
     * @throws NotFoundException when no object can be found for the specified objectId
     * @throws ObjectOutOfSyncException when the object was modified by another process before these changes could be committed
     */
    ObjectVersionId commitStagedChanges(String objectId, VersionInfo versionInfo);

    /**
     * Deletes the staged changes (mutable HEAD) of the specified object. If the object does not have staged changes, then
     * nothing happens.
     *
     * @param objectId the id of the object
     * @throws NotFoundException when no object can be found for the specified objectId
     */
    void purgeStagedChanges(String objectId);

    /**
     * Returns true if the object has staged changes (mutable HEAD).
     *
     * @param objectId the id of the object
     * @return if the object has staged changes
     * @throws NotFoundException when no object can be found for the specified objectId
     */
    boolean hasStagedChanges(String objectId);

}
