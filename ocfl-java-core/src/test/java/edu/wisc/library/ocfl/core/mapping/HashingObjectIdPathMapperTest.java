package edu.wisc.library.ocfl.core.mapping;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashingObjectIdPathMapperTest {

    private String digestAlgorithm = "sha-256";
    private String objectId = "http://library.wisc.edu/123";

    @Test
    public void shouldMap3LevelsDeepWith2CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 2, 2, false);
        var result = mapper.map(objectId);
        assertEquals("ed/75/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

    @Test
    public void shouldMap5LevelsDeepWith2CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 4, 2, false);
        var result = mapper.map(objectId);
        assertEquals("ed/75/58/5a/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

    @Test
    public void shouldMap2LevelsDeepWith3CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 2, 3, false);
        var result = mapper.map(objectId);
        assertEquals("ed7/558/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

}
