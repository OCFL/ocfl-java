package edu.wisc.library.ocfl.core.mapping;

/**
 * Generates the directory name to use to encapsulate an object within the storage hierarchy.
 */
public interface Encapsulator {

    /**
     * Returns the name of the directory to encapsulate an object within.
     *
     * @param objectId the original, unencoded, object id
     * @param encodedId the encoded object id
     * @return directory name
     */
    String encapsulate(String objectId, String encodedId);

}
