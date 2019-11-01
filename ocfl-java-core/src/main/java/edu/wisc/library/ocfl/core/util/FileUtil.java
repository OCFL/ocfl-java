package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.util.*;

public final class FileUtil {

    private FileUtil() {

    }

    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    private static final SecureRandom RANDOM = new SecureRandom();


    public static Path createDirectories(Path path) {
        try {
            return Files.createDirectories(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static Path createTempDir(Path rootPath, String prefix) {
        try {
            var name = URLEncoder.encode(prefix, StandardCharsets.UTF_8) + "-" + Long.toUnsignedString(RANDOM.nextLong());
            return Files.createDirectory(rootPath.resolve(name));
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void moveDirectory(Path srcRoot, Path dstRoot) {
        try {
            Files.move(srcRoot, dstRoot, StandardCopyOption.REPLACE_EXISTING);
            return;
        } catch (IOException e) {
            // Was unable to do a rename. Must do a deep copy.
        }

        if (!Files.exists(dstRoot)) {
            createDirectories(dstRoot);
        } else if (Files.isRegularFile(dstRoot)) {
            throw new IllegalArgumentException("Destination must be a directory: " + dstRoot);
        }

        try {
            Files.walkFileTree(srcRoot, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                private Path createDstPath(Path current) {
                    return dstRoot.resolve(srcRoot.relativize(current));
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(srcRoot)) {
                        Files.createDirectory(createDstPath(dir));
                    }
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.move(file, createDstPath(file), StandardCopyOption.REPLACE_EXISTING);
                    return super.visitFile(file, attrs);
                }

                @Override
                public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                    Files.delete(dir);
                    return super.postVisitDirectory(dir, exc);
                }
            });
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copy(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.copy(src, dst, copyOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void copyFileMakeParents(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.createDirectories(dst.getParent());
            Files.copy(src, dst, copyOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void moveFileMakeParents(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.createDirectories(dst.getParent());
            Files.move(src, dst, copyOptions);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void delete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void safeDeletePath(Path path) {
        if (Files.exists(path)) {
            try (var paths = Files.walk(path)) {
                paths.sorted(Comparator.reverseOrder())
                        .forEach(f -> {
                            try {
                                Files.delete(f);
                            } catch (NoSuchFileException e) {
                                // Ignore
                            } catch (IOException e) {
                                LOG.warn("Failed to delete file: {}", f, e);
                            }
                        });
            } catch (IOException e) {
                LOG.warn("Failed to delete directory: {}", path, e);
            }
        }
    }

    public static List<Path> findFiles(Path path) {
        var files = new ArrayList<Path>();

        if (Files.isDirectory(path)) {
            try (var paths = Files.walk(path)) {
                paths.filter(Files::isRegularFile)
                        .forEach(files::add);
            } catch (IOException e) {
                throw new RuntimeIOException(e);
            }
        } else {
            files.add(path);
        }

        return files;
    }

    public static StandardCopyOption[] toCopyOptions(OcflOption... ocflOptions) {
        var options = new HashSet<>(Arrays.asList(ocflOptions));
        if (options.contains(OcflOption.OVERWRITE)) {
            return new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING};
        }
        return new StandardCopyOption[] {};
    }


}
