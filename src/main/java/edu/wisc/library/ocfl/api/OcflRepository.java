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
     * @param objectId
     * @param path
     * @param commitMessage
     */
    // TODO this should probably return a reference to the new version that was created
    void putObject(String objectId, Path path, CommitMessage commitMessage);

    /**
     * Updates an existing object by selectively adding and removing files and creating a new version that encapsulates
     * all of the changes.
     *
     * @param objectId
     * @param commitMessage
     */
    // TODO this should probably return a reference to the new version that was created
    void updateObject(String objectId, CommitMessage commitMessage, Consumer<OcflObjectUpdater> objectUpdater);

    void getObject(String objectId, Path outputPath);

    void getObject(String objectId, String versionId, Path outputPath);

    // TODO perhaps there should be a getObject method that operates on a Consumer<>, providing a virtual view of an object
    //      this would be read-only. a read-write version would also be possible, but creates a bigger locking problem.

}
