package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Map;

/**
 * Object ids are first hashed and then partitioned into a specified number of segments of a specified length. Finally,
 * the entire hashed object id is used as the object encapsulation directory.
 *
 * TODO example
 */
public class HashingObjectIdPathMapper implements ObjectIdPathMapper {

    private int depth;
    private int segmentLength;
    private int minHashLength;
    private boolean useUppercase;

    private String digestAlgorithm;
    private ObjectIdPathMapper delegateMapper;

    /**
     * @param digestAlgorithm the digest algorithm to use on the object id
     * @param depth the number of directories deep that should be created
     * @param segmentLength the number of characters that should be in each directory name
     * @param useUppercase indicates whether the digest should be encoded using lowercase or uppercase characters
     */
    public HashingObjectIdPathMapper(String digestAlgorithm, int depth, int segmentLength, boolean useUppercase) {
        this.digestAlgorithm = Enforce.notBlank(digestAlgorithm, "digestAlgorithm");
        this.depth = Enforce.expressionTrue(depth >= 1, depth, "depth must be at least 0");
        this.segmentLength = Enforce.expressionTrue(segmentLength >= 1, segmentLength, "segmentLength must be at least 0");
        this.minHashLength = depth * segmentLength;
        this.useUppercase = useUppercase;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path map(String objectId) {
        var hashChars = Hex.encodeHex(
                DigestUtils.digest(DigestUtils.getDigest(digestAlgorithm), objectId.getBytes()),
                !useUppercase);

        if (hashChars.length < minHashLength) {
            throw new IllegalStateException("The hashed objectId does not contain enough characters to partition adequately.");
        }

        var objectPath = Paths.get("");

        for (int i = 0; i < minHashLength; i += segmentLength) {
            objectPath = objectPath.resolve(new String(Arrays.copyOfRange(hashChars, i, i + segmentLength)));
        }

        return objectPath.resolve(new String(hashChars));
    }

    @Override
    public Map<String, Object> describeLayout() {
        // TODO https://github.com/OCFL/spec/issues/351
        return Map.of(
                // TODO update to 'key' once the key is known
                "uri", String.format("https://birkland.github.io/ocfl-rfc-demo/0003-truncated-ntuple-layout?encoding=%s&depth=%s&n=%s",
                        digestAlgorithm, depth, segmentLength),
                "description", "Truncated n-tuple Layout"
        );
    }

}
