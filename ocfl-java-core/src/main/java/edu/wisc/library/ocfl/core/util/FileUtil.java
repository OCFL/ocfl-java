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
     * Attempts to rename the source directory to the destination directory, replacing the destination. If the rename fails,
     * the contents of the source directory are recursively moved into the destination directory file by file. Rename
     * should only fail if the move is happening across volumes.
     */
    public static void moveDirectory(Path srcRoot, Path dstRoot) {
        try {
            Files.move(srcRoot, dstRoot, StandardCopyOption.REPLACE_EXISTING);
            return;
        } catch (IOException e) {
            // Was unable to do a rename. Must do a deep copy.
            // Rename should only fail if a directory is being moved across volumes.
        }

        // TODO this is merging content into an existing directory rather than replacing. acceptable?
        // TODO if not, the dst directory should NOT be removed. instead, its children must be deleted
        if (!Files.exists(dstRoot)) {
            createDirectories(dstRoot);
        } else if (Files.isRegularFile(dstRoot)) {
            throw new IllegalArgumentException("Destination must be a directory: " + dstRoot);
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
