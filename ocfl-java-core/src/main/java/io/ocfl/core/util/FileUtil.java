/*
 * The MIT License (MIT)
 *
 * Copyright (c) 2019 University of Wisconsin Board of Regents
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package io.ocfl.core.util;

import io.ocfl.api.OcflOption;
import io.ocfl.api.exception.OcflIOException;
import io.ocfl.api.exception.OcflNoSuchFileException;
import io.ocfl.api.model.DigestAlgorithm;
import io.ocfl.api.util.Enforce;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class FileUtil {

    private FileUtil() {}

    private static final Logger LOG = LoggerFactory.getLogger(FileUtil.class);

    /**
     * Creates a new directory as a child of the parent path named: md5(objectId)-[random-long]
     *
     * <p>A hash is used to avoid problems with object ids that are longer than 255 characters.
     *
     * @param parent the path to create the new directory under
     * @param objectId the object id to create the directory for
     * @return the path to the new directory
     */
    public static Path createObjectTempDir(Path parent, String objectId) {
        var digest = DigestUtil.computeDigestHex(DigestAlgorithm.md5, objectId);
        UncheckedFiles.createDirectories(parent);
        return UncheckedFiles.createDirectory(parent.resolve(digest + "-"
                + Integer.toUnsignedString(ThreadLocalRandom.current().nextInt())));
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
            throw new IllegalArgumentException(
                    "Parent directory of destination must exist and be a directory: " + dstRoot.getParent());
        }

        try {
            Files.move(srcRoot, dstRoot, StandardCopyOption.ATOMIC_MOVE);
            return;
        } catch (AtomicMoveNotSupportedException e) {
            // This fails if the destination exists, the source and destination are on different volumes, or the
            // provider
            // does not support atomic moves.
            LOG.debug("Atomic move of {} to {} failed.", srcRoot, dstRoot, e);
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }

        try {
            Files.createDirectory(dstRoot);
        } catch (FileAlreadyExistsException e) {
            throw e;
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }

        try {
            Files.walkFileTree(
                    srcRoot, Set.of(FileVisitOption.FOLLOW_LINKS), Integer.MAX_VALUE, new SimpleFileVisitor<>() {
                        private Path dstPath(Path current) {
                            return dstRoot.resolve(srcRoot.relativize(current));
                        }

                        @Override
                        public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs)
                                throws IOException {
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
            throw OcflIOException.from(e);
        }
    }

    public static void recursiveCopy(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            Files.createDirectories(dst);
            Files.walkFileTree(src, new SimpleFileVisitor<>() {
                private Path dstPath(Path current) {
                    return dst.resolve(src.relativize(current));
                }

                @Override
                public FileVisitResult preVisitDirectory(Path dir, BasicFileAttributes attrs) throws IOException {
                    if (!dir.equals(src)) {
                        Files.createDirectories(dstPath(dir));
                    }
                    return super.preVisitDirectory(dir, attrs);
                }

                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                    Files.copy(file, dstPath(file), copyOptions);
                    return super.visitFile(file, attrs);
                }
            });
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    public static void copyFileMakeParents(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            if (Files.notExists(dst.getParent())) {
                Files.createDirectories(dst.getParent());
            }
            Files.copy(src, dst, copyOptions);
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    public static void moveFileMakeParents(Path src, Path dst, StandardCopyOption... copyOptions) {
        try {
            if (Files.notExists(dst.getParent())) {
                Files.createDirectories(dst.getParent());
            }
            Files.move(src, dst, copyOptions);
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    /**
     * Recursively deletes all of the children of the specified path. The root path itself is not deleted.
     *
     * @param root the path under which all children should be deleted
     */
    public static void deleteChildren(Path root) {
        try (var files = Files.walk(root)) {
            files.sorted(Comparator.reverseOrder()).filter(f -> !f.equals(root)).forEach(UncheckedFiles::delete);
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    /**
     * Deletes any empty directories under the specified path
     *
     * @param root the path to prune empty child directories from
     */
    public static void deleteEmptyDirs(Path root) {
        try (var files = Files.find(root, Integer.MAX_VALUE, (file, attrs) -> attrs.isDirectory())) {
            files.filter(f -> !f.equals(root)).sorted(Comparator.reverseOrder()).forEach(FileUtil::deleteDirIfEmpty);
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    /**
     * Deletes the specified directory and any of its parents that are empty. The search terminates as soon as
     * a non-empty parent directory is encountered.
     *
     * @param path the path do delete
     */
    public static void deleteDirAndParentsIfEmpty(Path path) {
        if (deleteDirIfEmpty(path)) {
            deleteDirAndParentsIfEmpty(path.getParent());
        }
    }

    /**
     * Deletes the specified path and all of its ancestors until the stop directory is reached
     *
     * @param path directory to delete
     * @param stop directory to stop at
     */
    public static void deleteDirAndParentsIfEmpty(Path path, Path stop) {
        if (path.equals(stop)) {
            return;
        }

        if (deleteDirIfEmpty(path)) {
            deleteDirAndParentsIfEmpty(path.getParent(), stop);
        }
    }

    /**
     * Attempts to delete the specified directory and ignores failures related to the directory not existing or
     * not empty
     *
     * @param directory directory to delete
     * @return true if the directory was deleted
     */
    public static boolean deleteDirIfEmpty(Path directory) {
        try {
            Files.delete(directory);
            return true;
        } catch (NoSuchFileException | FileNotFoundException e) {
            return true;
        } catch (DirectoryNotEmptyException e) {
            return false;
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    /**
     * Deletes the path and all of its children. Any exception that is thrown is logged as a warning.
     *
     * @param path the path to delete
     */
    public static void safeDeleteDirectory(Path path) {
        try (var paths = Files.walk(path)) {
            paths.sorted(Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.delete(f);
                } catch (NoSuchFileException e) {
                    // Ignore
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", f, e);
                }
            });
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            LOG.warn("Failed to delete directory: {}", path, e);
        }
    }

    /**
     * Non-recursive path delete. Any exception is swallowed and logged.
     *
     * @param path the path to delete
     */
    public static void safeDelete(Path path) {
        try {
            Files.delete(path);
        } catch (IOException e) {
            LOG.warn("Failed to delete path: {}", path, e);
        }
    }

    /**
     * Recursive delete that deletes everything under and including the specified directory. Failures deleting individual
     * files are logged and a single exception is thrown at the end.
     *
     * @param directory the directory to delete
     */
    public static void deleteDirectory(Path directory) {
        var hasErrors = new AtomicBoolean(false);

        try (var paths = Files.walk(directory)) {
            paths.sorted(Comparator.reverseOrder()).forEach(f -> {
                try {
                    Files.delete(f);
                } catch (NoSuchFileException e) {
                    // ignore
                } catch (IOException e) {
                    LOG.warn("Failed to delete file: {}", f, e);
                    hasErrors.set(true);
                }
            });
        } catch (NoSuchFileException e) {
            // ignore
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }

        if (hasErrors.get()) {
            throw new OcflIOException(
                    String.format("Failed to recursively delete directory %s. See logs for details.", directory));
        }
    }

    public static boolean isDirEmpty(Path path) {
        try (var stream = Files.newDirectoryStream(path)) {
            return !stream.iterator().hasNext();
        } catch (IOException e) {
            throw OcflIOException.from(e);
        }
    }

    public static List<Path> findFiles(Path path) {
        var files = new ArrayList<Path>();

        if (Files.isDirectory(path, LinkOption.NOFOLLOW_LINKS)) {
            try (var paths = Files.find(path, Integer.MAX_VALUE, (file, attrs) -> attrs.isRegularFile())) {
                paths.forEach(files::add);
            } catch (IOException e) {
                throw OcflIOException.from(e);
            }
        } else {
            files.add(path);
        }

        return files;
    }

    public static StandardCopyOption[] toCopyOptions(OcflOption... options) {
        if (OcflOption.contains(OcflOption.OVERWRITE, options)) {
            return new StandardCopyOption[] {StandardCopyOption.REPLACE_EXISTING};
        }
        return new StandardCopyOption[] {};
    }

    public static boolean hasChildren(Path path) {
        try {
            return !FileUtil.isDirEmpty(path);
        } catch (OcflNoSuchFileException e) {
            return false;
        }
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
                throw new IllegalArgumentException(String.format(
                        "Path cannot be joined because it contains empty parts: %s", Arrays.asList(parts)));
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
                    throw new IllegalArgumentException(String.format(
                            "Path cannot be joined because it contains empty parts: %s", Arrays.asList(parts)));
                }
            }
        }

        return pathBuilder.toString();
    }

    /**
     * Returns the path to the parent of the specified path, using forward slashes as path separators. If the path
     * has no parent, then an empty string is returned.
     *
     * @param path path
     * @return the path's parent
     */
    public static String parentPath(String path) {
        var lastIndex = path.lastIndexOf('/');
        if (lastIndex > -1) {
            return path.substring(0, lastIndex);
        }
        return "";
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
