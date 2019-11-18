package edu.wisc.library.ocfl.core.mapping;

import java.nio.file.Path;
import java.util.Map;

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

    /**
     * Returns map containing a description of the file layout. At the minimum, this map MUST contain 'uri' and 'description'
     * entries.
     *
     * @return map describing the file layout
     */
    Map<String, Object> describeLayout();

}
