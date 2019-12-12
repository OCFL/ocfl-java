package edu.wisc.library.ocfl.core.util;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public final class DigestUtil {

    private static final int BUFFER_SIZE = 8192;

    private DigestUtil() {

    }

    public static String computeDigestHex(DigestAlgorithm algorithm, Path path) {
        return computeDigestHex(algorithm, path, false);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, Path path, boolean upperCase) {
        return Bytes.from(computeDigest(algorithm, path)).encodeHex(upperCase);
    }

    public static byte[] computeDigest(DigestAlgorithm algorithm, Path path) {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var digest = algorithm.getMessageDigest();
            var buffer = ByteBuffer.allocateDirect(BUFFER_SIZE);

            while (channel.read(buffer) > -1) {
                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            return digest.digest();
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, String value) {
        return computeDigestHex(algorithm, value, false);
    }

    public static String computeDigestHex(DigestAlgorithm algorithm, String value, boolean upperCase) {
        return Bytes.from(value).hash(algorithm.getJavaStandardName()).encodeHex(upperCase);
    }

}
