package io.ocfl.api.model;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import io.ocfl.api.DigestAlgorithmRegistry;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Map;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class JacksonTest {

    private ObjectMapper objectMapper;

    @BeforeEach
    public void setup() {
        objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());
    }

    @Test
    public void roundTripObjectDetails() throws JsonProcessingException {
        var original = new ObjectDetails()
                .setId("id")
                .setDigestAlgorithm(DigestAlgorithmRegistry.sha512)
                .setHeadVersionNum(VersionNum.V1)
                .setVersions(Map.of(
                        VersionNum.V1,
                        new VersionDetails()
                                .setObjectVersionId(ObjectVersionId.version("id", 1))
                                .setCreated(OffsetDateTime.now(ZoneOffset.UTC))
                                .setVersionInfo(new VersionInfo()
                                        .setUser("me", "mailto:me@example.com")
                                        .setMessage("commit")
                                        .setCreated(OffsetDateTime.now(ZoneOffset.UTC)))
                                .setFileMap(Map.of(
                                        "file.txt",
                                        new FileDetails()
                                                .setPath("file.txt")
                                                .setStorageRelativePath("object/file.txt")
                                                .setFixity(Map.of(DigestAlgorithmRegistry.sha512, "abc123"))))));

        var json = objectMapper.writeValueAsString(original);

        var result = objectMapper.readValue(json, ObjectDetails.class);

        assertEquals(original, result);
    }
}
