package edu.wisc.library.ocfl.core.mapping;

import java.nio.file.Path;

/**
 * Extension point for mapping object ids to paths within the OCFL root.
 */
public interface ObjectIdPathMapper {

    /**
     * Maps an objectId to its path within the OCFL root
     *
     * @param objectId
     * @return
     */
    Path map(String objectId);

}
