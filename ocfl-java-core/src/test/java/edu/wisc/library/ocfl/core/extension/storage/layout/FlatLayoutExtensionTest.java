package edu.wisc.library.ocfl.core.extension.storage.layout;

import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertEquals;

import edu.wisc.library.ocfl.api.exception.OcflExtensionException;
import java.nio.file.FileSystems;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FlatLayoutExtensionTest {

    private FlatLayoutExtension ext;

    @BeforeEach
    public void setup() {
        ext = new FlatLayoutExtension();
    }

    @Test
    public void mapObjectIdWhenOnlyContainsSafeChars() {
        var objectId = "obj123";
        assertEquals(objectId, ext.mapObjectId(objectId));
    }

    @Test
    public void mapObjectIdWhenOnlyContainsSpecialChars() {
        var objectId = "..hor_rib:lé-$id";
        assertEquals(objectId, ext.mapObjectId(objectId));
    }

    @Test
    public void failWhenIdContainsPathSeparator() {
        var objectId = "obj" + FileSystems.getDefault().getSeparator().charAt(0) + "123";
        assertThatThrownBy(() -> {
                    ext.mapObjectId(objectId);
                })
                .isInstanceOf(OcflExtensionException.class)
                .hasMessageContaining("path separator");
    }
}
