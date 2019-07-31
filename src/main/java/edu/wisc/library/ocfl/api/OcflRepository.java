package edu.wisc.library.ocfl.api;

import java.nio.file.Path;

public interface OcflRepository {

    // TODO support specifying multiple paths? how to handle the merge?
    void putObject(String objectId, Path path, CommitMessage commitMessage);

    void getObject(String objectId, Path outputPath);

    void getObject(String objectId, String versionId, Path outputPath);

}
