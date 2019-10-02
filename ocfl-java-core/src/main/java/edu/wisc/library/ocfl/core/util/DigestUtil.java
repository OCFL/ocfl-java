package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.core.model.DigestAlgorithm;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;

import java.io.IOException;
import java.nio.file.Path;

public final class DigestUtil {

    private DigestUtil() {

    }

    public static String computeDigest(DigestAlgorithm algorithm, Path path) {
        try {
            return Hex.encodeHexString(DigestUtils.digest(algorithm.getMessageDigest(), path.toFile()));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

}
