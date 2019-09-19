package edu.wisc.library.ocfl.api;

public enum OcflOption {

    /**
     * Instructs an update operation to overwrite existing files in an object.
     */
    OVERWRITE,
    /**
     * Instructs an update operation to move files into the repository instead of copying them.
     */
    MOVE_SOURCE
}
