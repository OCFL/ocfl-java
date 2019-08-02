package edu.wisc.library.ocfl.api;

import java.nio.file.Path;
import java.util.function.Consumer;

public interface OcflRepository {

    /**
     * Adds the object rooted at the given path to the OCFL repository under the given objectId. If their is an existing
     * object with the id, then a new version of the object is created.
     *
     * It is important to note that the files present in the given path should comprise the entirety of the object. Files
     * from previous versions that are no longer present in the path are not carried over. At the same time, files that
     * are unchanged between object versions are not stored a second time.
     *
     * This method should only be used when writing new or fully composed objects.
     *
     * If the current head version of the object does not match the version specified in the request, the update will
     * be rejected. If the request specifies the HEAD version, then no version check will be preformed.
     *
     * @param objectId
     * @param path
     * @param commitMessage
     */
    ObjectId putObject(ObjectId objectId, Path path, CommitMessage commitMessage);

    /**
     * Updates an existing object by selectively adding and removing files and creating a new version that encapsulates
     * all of the changes.
     *
     * If the current head version of the object does not match the version specified in the request, the update will
     * be rejected. If the request specifies the HEAD version, then no version check will be preformed.
     *
     * @param objectId
     * @param commitMessage
     */
    ObjectId updateObject(ObjectId objectId, CommitMessage commitMessage, Consumer<OcflObjectUpdater> objectUpdater);

    void getObject(ObjectId objectId, Path outputPath);

    void readObject(ObjectId objectId, Consumer<OcflObjectReader> objectReader);

    // TODO consider adding a read-write version of the updateObject/readObject APIs.

    // TODO describeObject

    // TODO rollbackObject

}
