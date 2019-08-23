package edu.wisc.library.ocfl.core.mapping;

import java.nio.file.Path;

public interface ObjectIdPathMapper {

    Path map(String objectId);

}
