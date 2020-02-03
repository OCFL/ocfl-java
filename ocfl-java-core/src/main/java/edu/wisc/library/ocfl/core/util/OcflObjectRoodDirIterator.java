package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Iterator that iterates over OCFL object root directories. Object roots are identified by the presence of a file that's
 * prefixed with '0=ocfl_object'.
 */
// TODO needs more tests!
class OcflObjectRoodDirIterator implements Iterator<Path>, Closeable {

    private static final String OCFL_OBJECT_MARKER_PREFIX = "0=ocfl_object";

    private Path start;
    private boolean started = false;
    private boolean closed = false;

    private ArrayDeque<Directory> dirStack;
    private Path next;

    OcflObjectRoodDirIterator(Path start) {
        this.start = Enforce.notNull(start, "start cannot be null");
        this.dirStack = new ArrayDeque<>();
    }

    @Override
    public void close() {
        if (!closed) {
            while (!dirStack.isEmpty()) {
                popDirectory();
            }
            closed = true;
        }
    }

    @Override
    public boolean hasNext() {
        if (closed) {
            throw new IllegalStateException("Iterator is closed.");
        }
        fetchNextIfNeeded();
        return next != null;
    }

    @Override
    public Path next() {
        if (!hasNext()) {
            throw new NoSuchElementException("No more files found.");
        }
        var result = next;
        next = null;
        return result;
    }

    private void fetchNextIfNeeded() {
        try {
            if (next == null) {
                var nextDirectory = fetchNextDirectory();

                while (nextDirectory != null) {
                    var children = Files.newDirectoryStream(nextDirectory, p -> {
                        return p.getFileName().toString().startsWith(OCFL_OBJECT_MARKER_PREFIX);
                    });

                    // Found OCFL object marker -- current directory is an OCFL object root
                    if (children.iterator().hasNext()) {
                        // Do not process children
                        popDirectory();
                        next = nextDirectory;
                        return;
                    }

                    nextDirectory = fetchNextDirectory();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private Path fetchNextDirectory() {
        if (!started) {
            dirStack.push(new Directory(start));
            started = true;
        }

        var top = dirStack.peek();

        while (top != null) {
            var child = top.nextChild();

            if (child == null) {
                popDirectory();
                top = dirStack.peek();
            } else if (Files.isDirectory(child, LinkOption.NOFOLLOW_LINKS)) {
                dirStack.push(new Directory(child));
                return child;
            }
        }

        return null;
    }

    private void popDirectory() {
        if (!dirStack.isEmpty()) {
            var top = dirStack.pop();
            top.close();
        }
    }

    private static class Directory {

        private Path path;
        private DirectoryStream<Path> stream;
        private Iterator<Path> children;

        Directory(Path path) {
            try {
                this.path = path;
                this.stream = Files.newDirectoryStream(path);
                this.children = stream.iterator();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        Path nextChild() {
            if (children.hasNext()) {
                return children.next();
            }
            return null;
        }

        void close() {
            try {
                stream.close();
            } catch (IOException e) {
                // ignore
            }
        }

    }

}
