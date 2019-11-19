package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.OcflOption;
import edu.wisc.library.ocfl.api.exception.RuntimeIOException;
import edu.wisc.library.ocfl.api.util.Enforce;
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

    /**
     * Performs an atomic directory move. The srcRoot must exist and the dstRoot must NOT exist.
     *
     * <p>First, an atomic move (rename) is attempted. If this fails, because the source and destination are on different
     * volumes, then the destination directory is created and the source files are recursively moved to the destination.
     *
     * @param srcRoot source directory to move, most exist
     * @param dstRoot destination directory, must NOT exist but its parent must exist
     * @throws FileAlreadyExistsException when the move fails because the destination already exists
     */
    public static void moveDirectory(Path srcRoot, Path dstRoot) throws FileAlreadyExistsException {
        if (Files.notExists(srcRoot) || Files.isRegularFile(srcRoot)) {
            throw new IllegalArgumentException("Source must exist and be a directory: " + srcRoot);
        }
        if (Files.exists(dstRoot)) {
            // Linux rename supports overwriting an empty destination that exists, but this does not work on Windows
            throw new FileAlreadyExistsException("Destination must not exist: " + dstRoot);
        }
        if (Files.notExists(dstRoot.getParent()) || Files.isRegularFile(dstRoot.getParent())) {
            throw new IllegalArgumentException("Parent directory of destination must exist and be a directory: " + dstRoot.getParent());
        }

        try {
            Files.move(srcRoot, dstRoot, StandardCopyOption.ATOMIC_MOVE);
            return;
        } catch (AtomicMoveNotSupportedException e) {
            // This fails if the destination exists, the source and destination are on different volumes, or the provider
            // does not support atomic moves.
            LOG.debug("Atomic move of {} to {} failed.", srcRoot, dstRoot, e);
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        try {
            Files.createDirectory(dstRoot);
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }

        try {
            Files.walkFileTree(srcRoot, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                private Path dstPath(Path current) {
                    return dstRoot.resolve(srcRoot.relativize(current));
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(srcRoot)) {
                        Files.createDirectories(dstPath(dir));
                    }
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.move(file, dstPath(file), StandardCopyOption.REPLACE_EXISTING);
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

    public static void deleteChildren(Path root) {
        try (var files = Files.walk(root)) {
            files.sorted(Comparator.reverseOrder())
                    .filter(f -> !f.equals(root))
                    .forEach(FileUtil::delete);
        } catch (IOException e) {
            throw new RuntimeIOException(e);
        }
    }

    public static void deleteEmptyDirs(Path root) {
        try (var files = Files.walk(root)) {
            files.filter(Files::isDirectory)
                    .filter(f -> !f.equals(root))
                    .filter(f -> f.toFile().list().length == 0)
                    .forEach(FileUtil::delete);
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

    public static boolean hasChildren(Path path) {
        if (Files.exists(path) && Files.isDirectory(path)) {
            var list = path.toFile().list();
            return list != null && list.length > 0;
        }
        return false;
    }

    /**
     * Returns a string representation of a Path that uses '/' as the file separator.
     *
     * @param path the path to represent as a string
     * @return path using '/' as the separator
     */
    public static String pathToStringStandardSeparator(Path path) {
        Enforce.notNull(path, "path cannot be null");
        var separator = path.getFileSystem().getSeparator().charAt(0);
        var pathStr = path.toString();

        if (separator == '/') {
            return pathStr;
        }
        return pathStr.replace(separator, '/');
    }

    /**
     * Joins all of the parts together as a path separated by '/'. Leading and trailing slashes on path parts are normalized,
     * but slashes within parts are not changed. Empty parts are ignored.
     *
     * @param parts the path parts to join
     * @return joined path with empty elements left out
     */
    public static String pathJoinIgnoreEmpty(String... parts) {
        return pathJoin(false, parts);
    }

    /**
     * Joins all of the parts together as a path separated by '/'. Leading and trailing slashes on path parts are normalized,
     * but slashes within parts are not changed. Throws an IllegalArgumentException if empty parts are encountered.
     *
     * @param parts the path parts to join
     * @return joined path
     */
    public static String pathJoinFailEmpty(String... parts) {
        return pathJoin(true, parts);
    }

    private static String pathJoin(boolean failOnEmpty, String... parts) {
        if (parts == null || parts.length == 0) {
            return "";
        }

        var pathBuilder = new StringBuilder();
        var addSeparator = false;

        for (var i = 0; i < parts.length; i++) {
            var part = parts[i];

            if (failOnEmpty && (part == null || part.isEmpty())) {
                throw new IllegalArgumentException(String.format("Path cannot be joined because it contains empty parts: %s", Arrays.asList(parts)));
            }

            if (part != null && !part.isEmpty()) {
                String strippedPart;

                if (i == 0) {
                    strippedPart = firstPathPart(part);
                } else {
                    strippedPart = stripSlashes(part);
                }

                if (!strippedPart.isEmpty()) {
                    if (addSeparator) {
                        pathBuilder.append("/");
                    }
                    pathBuilder.append(strippedPart);
                    addSeparator = true;
                } else if (failOnEmpty) {
                    throw new IllegalArgumentException(String.format("Path cannot be joined because it contains empty parts: %s", Arrays.asList(parts)));
                }
            }
        }

        return pathBuilder.toString();
    }

    private static String stripSlashes(String path) {
        int startIndex;
        int endIndex = path.length();

        for (startIndex = 0; startIndex < path.length(); startIndex++) {
            if (path.charAt(startIndex) != '/') {
                break;
            }
        }

        if (startIndex != path.length()) {
            for (endIndex = path.length(); endIndex > 0; endIndex--) {
                if (path.charAt(endIndex - 1) != '/') {
                    break;
                }
            }
        }

        if (startIndex == path.length()) {
            // no non-slash chars
            return "";
        } else if (startIndex == 0 && endIndex == path.length()) {
            // no leading or trailing slash
            return path;
        } else if (endIndex == path.length()) {
            // leading slash
            return path.substring(startIndex);
        } else {
            return path.substring(startIndex, endIndex);
        }
    }

    private static String firstPathPart(String path) {
        var stripped = stripSlashes(path);
        if (path.charAt(0) == '/') {
            return "/" + stripped;
        }
        return stripped;
    }

}
