package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;

/**
 * Object ids are first hashed and then partitioned into a specified number of segments of a specified length. Finally,
 * the entire hashed object id is used as the object encapsulation directory.
 *
 * TODO example
 */
public class HashingObjectIdPathMapper implements ObjectIdPathMapper {

    private int partitionCount;
    private int partitionLength;
    private int minHashLength;

    private String digestAlgorithm;
    private ObjectIdPathMapper delegateMapper;

    /**
     * @param digestAlgorithm The digest algorithm to use on the object id
     * @param partitionCount The number of directories deep that should be created
     * @param partitionLength The number of characters that should be in each directory name
     */
    public HashingObjectIdPathMapper(String digestAlgorithm, int partitionCount, int partitionLength) {
        this.digestAlgorithm = Enforce.notBlank(digestAlgorithm, "digestAlgorithm");
        this.partitionCount = Enforce.expressionTrue(partitionCount >= 1, partitionCount, "partitionCount must be at least 0");
        this.partitionLength = Enforce.expressionTrue(partitionLength >= 1, partitionLength, "partitionLength must be at least 0");
        this.minHashLength = partitionCount * partitionLength;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Path map(String objectId) {
        var hashChars = Hex.encodeHex(DigestUtils.digest(DigestUtils.getDigest(digestAlgorithm), objectId.getBytes()));

        if (hashChars.length < minHashLength) {
            throw new IllegalStateException("The hashed objectId does not contain enough characters to partition adequately.");
        }

        var objectPath = Paths.get("");

        for (int i = 0; i < minHashLength; i += partitionLength) {
            objectPath = objectPath.resolve(new String(Arrays.copyOfRange(hashChars, i, i + partitionLength)));
        }

        return objectPath.resolve(new String(hashChars));
    }

}
