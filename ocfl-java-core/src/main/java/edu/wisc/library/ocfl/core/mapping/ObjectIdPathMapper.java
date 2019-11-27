package edu.wisc.library.ocfl.core.mapping;

/**
 * Extension point for mapping object ids to paths within the OCFL root.
 */
public interface ObjectIdPathMapper {

    /**
     * Maps an objectId to its path within the OCFL root
     *
     * @param objectId object id
     * @return the path to the object root relative to the OCFL root
     */
    String map(String objectId);

}
