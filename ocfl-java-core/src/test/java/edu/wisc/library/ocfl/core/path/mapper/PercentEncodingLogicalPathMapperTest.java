package edu.wisc.library.ocfl.core.path.mapper;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

public class PercentEncodingLogicalPathMapperTest {

    private static final String PATH = "tést/<bad>:Path 1/\\|obj/?8*%id/#{something}/[0]/۞.txt ";

    @Test
    public void encodeWindowsChars() {
        assertDefaultPath(
                LogicalPathMappers.percentEncodingWindowsMapper(),
                "tést/%3cbad%3e%3aPath%201/%5c%7cobj/%3f8%2a%25id/#{something}/[0]/۞.txt%20");
    }

    @Test
    public void encodeLinuxChars() {
        assertDefaultPath(
                LogicalPathMappers.percentEncodingLinuxMapper(),
                "tést/<bad>:Path%201/\\|obj/?8*%25id/#{something}/[0]/۞.txt%20");
    }

    @Test
    public void encodeCloudChars() {
        assertDefaultPath(
                LogicalPathMappers.percentEncodingCloudMapper(),
                "tést/<bad>:Path%201/%5c|obj/%3f8%2a%25id/%23{something}/%5b0%5d/۞.txt%20");
    }

    @Test
    public void encodeAllChars() {
        assertDefaultPath(
                LogicalPathMappers.percentEncodingAllMapper(),
                "tést/%3cbad%3e%3aPath%201/%5c%7cobj/%3f8%2a%25id/%23{something}/%5b0%5d/۞.txt%20");
    }

    @Test
    public void encodeConservativeChars() {
        assertDefaultPath(
                LogicalPathMappers.conservativePercentEncodingMapper(),
                "t%c3%a9st%2f%3cbad%3e%3aPath%201%2f%5c%7cobj%2f%3f8%2a%25id%2f%23%7bsomething%7d%2f%5b0%5d%2f%db%9e%2etxt%20");
    }

    private void assertDefaultPath(LogicalPathMapper mapper, String expected) {
        assertEquals(expected, mapper.toContentPathPart(PATH));
    }
}
