package edu.wisc.library.ocfl.api.io;

import at.favre.lib.bytes.Bytes;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class FixityCheckChannelTest {

    @TempDir
    public Path tempDir;

    @Test
    public void shouldCopyFileAndComputeDigestWhenEnabled() throws IOException {
        var path = writeFile("test.txt", "This is a test file");
        var expectedDigest = computeDigest(DigestAlgorithm.md5, path);

        var outPath = tempDir.resolve("test-copy.txt");

        var fileChannel = FileChannel.open(path, StandardOpenOption.READ);
        try (var fixityChannel = new FixityCheckChannel(fileChannel, DigestAlgorithm.md5, expectedDigest)) {
            transfer(fixityChannel, outPath);
            fixityChannel.checkFixity();
        }

        assertEquals(expectedDigest, computeDigest(DigestAlgorithm.md5, outPath));
    }

    private void transfer(ReadableByteChannel srcChannel, Path dstPath) throws IOException {
        try (var dstChannel = FileChannel.open(dstPath, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            long pos = 0;
            long r;
            while ((r = dstChannel.transferFrom(srcChannel, pos, Long.MAX_VALUE)) > 0) {
                pos += r;
            }
        }
    }

    private String computeDigest(DigestAlgorithm algorithm, Path path) {
        try (var channel = FileChannel.open(path, StandardOpenOption.READ)) {
            var digest = algorithm.getMessageDigest();
            var buffer = ByteBuffer.allocateDirect(8192);

            while (channel.read(buffer) > -1) {
                buffer.flip();
                digest.update(buffer);
                buffer.clear();
            }

            return Bytes.wrap(digest.digest()).encodeHex();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path writeFile(String name, String content) throws IOException {
        var path = tempDir.resolve(name);
        Files.writeString(path, content);
        return path;
    }

}
