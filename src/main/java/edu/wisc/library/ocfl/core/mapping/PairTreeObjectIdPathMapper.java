package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.CharArrayWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Set;

/**
 * An implementation of the Pairtree spec as described:
 *
 * <ul>
 * <li>https://github.com/birkland/ocfl-rfc-demo/blob/master/docs/0001-pairtree-layout.md</li>
 * <li>https://tools.ietf.org/html/draft-kunze-pairtree-01</li>
 * </ul>
 *
 * It can be configured to use either pairtree cleaning or url encoding. Url encoding should not be used when working
 * with an id space that makes frequent use of periods. The periods are not encoded, which can result in invalid directory
 * names.
 *
 * The advantage of the pairtree algorithm is that the paths can be resolved back to object ids. The disadvantage is that
 * it creates deep, unbalanced trees. If performance is a concern, HashingObjectIdPathMapper should be used.
 */
public class PairTreeObjectIdPathMapper implements ObjectIdPathMapper {

    private static final Set<String> INVALID_PATHS = Set.of(".", "..");

    private Encoder encoder;
    private String defaultEncapsulationName;
    private int encapsulationSubstringLength;

    /**
     * @param encoder The algorithm to use to encode an identifier
     * @param defaultEncapsulationName The directory name to use to encapsulate an object when the encoded identifier is less than 3 characters long
     * @param encapsulationSubstringLength The number of characters from the end of an encoded identifier to use to encapsulate an object
     */
    public PairTreeObjectIdPathMapper(Encoder encoder, String defaultEncapsulationName, int encapsulationSubstringLength) {
        this.encoder = Enforce.notNull(encoder, "encoder cannot be null");
        this.defaultEncapsulationName = Enforce.notBlank(defaultEncapsulationName, "defaultEncapsulationName cannot be blank");
        this.encapsulationSubstringLength = Enforce.expressionTrue(encapsulationSubstringLength > 2,
                encapsulationSubstringLength, "encapsulationSubstringLength must be greater than 2");
    }

    @Override
    public Path map(String objectId) {
        var encoded = encoder.encode(objectId);

        if (encoded.length() < 3) {
            return Path.of(encoded, defaultEncapsulationName);
        }

        var charArrayWriter = new CharArrayWriter();
        var parts = new String[(int) (Math.ceil(encoded.length() / 2.0) + 1)];
        var partIndex = 0;

        for (int i = 0; i < encoded.length(); i++) {
            charArrayWriter.write(encoded.charAt(i));
            if ((i + 1) % 2 == 0) {
                parts[partIndex++] = validateDir(objectId, charArrayWriter.toString());
                charArrayWriter.reset();
            }
        }

        if (charArrayWriter.size() > 0) {
            parts[partIndex++] = validateDir(objectId, charArrayWriter.toString());
        }

        if (encoded.length() < encapsulationSubstringLength) {
            parts[partIndex] = validateDir(objectId, encoded);
        } else {
            parts[partIndex] = validateDir(objectId, encoded.substring(encoded.length() - encapsulationSubstringLength));
        }

        return Paths.get(parts[0], Arrays.copyOfRange(parts, 1, parts.length));
    }

    // TODO should directories that start with a dot be allowed?
    private String validateDir(String objectId, String dir) {
        if (INVALID_PATHS.contains(dir)) {
            throw new IllegalArgumentException(String.format("Object id %s cannot be converted into a valid path.", objectId));
        }

        return dir;
    }

}
