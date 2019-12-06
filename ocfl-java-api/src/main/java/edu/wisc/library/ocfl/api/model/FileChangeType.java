package edu.wisc.library.ocfl.api.model;

/**
 * The type of change that occurred to a file in a version
 */
public enum FileChangeType {

    /**
     * Indicates a file content change. This includes new files, changes to existing files, and reinstated files
     */
    UPDATE,
    /**
     * Indicates the file was removed
     */
    REMOVE

}
