package edu.wisc.library.ocfl.api;

import edu.wisc.library.ocfl.api.exception.NotFoundException;
import edu.wisc.library.ocfl.api.exception.ObjectOutOfSyncException;
import edu.wisc.library.ocfl.api.model.CommitInfo;
import edu.wisc.library.ocfl.api.model.ObjectId;

import java.util.function.Consumer;

/**
 * Defines APIs for implementing the OCFL Mutable HEAD Extension. These APIs are outside of the scope of the core OCFL
 * specification, and provide additional functionality for staging object content within the OCFL storage root before
 * committing it to a version. It is imperative to understand that staged content is NOT part of the core OCFL object,
 * and it CANNOT be interpreted by an OCFL client that does not implement the Mutable HEAD Extension.
 */
public interface MutableOcflRepository extends OcflRepository {

    // TODO resolve language differences between "mutable HEAD version" and "staged version"

    /**
     * Creates or updates the mutable HEAD version of the specified object. If there is an existing mutable version, then
     * the changes are applied on top of the existing version, without creating a new version.
     *
     * <p>The changes contained in the mutable HEAD version are NOT part of the core OCFL object. Use {@code commitStagedVersion()}
     * to convert the mutable version into an immutable version that's part of the core OCFL object. This should be done
     * whenever possible.
     *
     * <p>If the current HEAD version of the object does not match the version specified in the request, the update will
     * be rejected. If the request specifies the HEAD version, then no version check will be preformed.
     *
     * @param objectId the id of the object. If set to a specific version, then the update will only occur
     *                 if this version matches the head object version in the repository.
     * @param commitInfo information about the changes to the object. Can be null.
     * @param objectUpdater code block within which updates to an object may be made
     * @return The objectId and version of the new object version
     * @throws NotFoundException when no object can be found for the specified objectId
     * @throws ObjectOutOfSyncException when the object was modified by another process before these changes could be committed
     */
    ObjectId stageChanges(ObjectId objectId, CommitInfo commitInfo, Consumer<OcflObjectUpdater> objectUpdater);

    /**
     * Converts the mutable HEAD version into an immutable core OCFL version that can be read by any OCFL client.
     *
     * <p>This operation will fail if any object versions were created between the time the mutable HEAD version was created and
     * when it was committed. To resolve this problem, the staged version must either be purged using {@code purgeStagedVersion()},
     * or the object must be manually edited to resolve the version conflict.
     *
     * @param objectId the id of the object
     * @param commitInfo information about the changes to the object. Can be null.
     * @return The objectId and version of the committed version
     * @throws NotFoundException when no object can be found for the specified objectId
     * @throws ObjectOutOfSyncException when the object was modified by another process before these changes could be committed
     * // TODO modeled exception when there is no staged version?
     */
    ObjectId commitStagedVersion(String objectId, CommitInfo commitInfo);

    /**
     * Deletes the mutable HEAD version of the specified object. If the object does not have a mutable HEAD version, then
     * nothing happens.
     *
     * @param objectId the id of the object
     * @throws NotFoundException when no object can be found for the specified objectId
     */
    void purgeStagedVersion(String objectId);

    /**
     * Returns true if the object has a mutable HEAD version.
     *
     * @param objectId the id of the object
     * @return if the object has a staged version
     * @throws NotFoundException when no object can be found for the specified objectId
     */
    boolean hasStagedVersion(String objectId);

}
