package edu.wisc.library.ocfl.core.mapping;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.nio.file.Paths;
import java.security.Security;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class HashingObjectIdPathMapperTest {

    private String digestAlgorithm = "blake2s-128";
    private Encoder encoder = new UrlEncoder(false);

    private String objectId = "http://library.wisc.edu/123";

    @BeforeAll
    public static void beforeAll() {
        Security.addProvider(new BouncyCastleProvider());
    }

    @Test
    public void shouldMap3LevelsDeepWith2CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 2, 2, encoder);
        var result = mapper.map(objectId);
        assertEquals(Paths.get("3e/a7/http%3a%2f%2flibrary.wisc.edu%2f123"), result);
    }

    @Test
    public void shouldMap5LevelsDeepWith2CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 4, 2, encoder);
        var result = mapper.map(objectId);
        assertEquals(Paths.get("3e/a7/10/ea/http%3a%2f%2flibrary.wisc.edu%2f123"), result);
    }

    @Test
    public void shouldMap2LevelsDeepWith3CharDirNames() {
        var mapper = new HashingObjectIdPathMapper(digestAlgorithm, 2, 3, encoder);
        var result = mapper.map(objectId);
        assertEquals(Paths.get("3ea/710/http%3a%2f%2flibrary.wisc.edu%2f123"), result);
    }

}
