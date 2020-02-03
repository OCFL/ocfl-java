package edu.wisc.library.ocfl.core.util;

import edu.wisc.library.ocfl.api.util.Enforce;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.FileVisitOption;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.NoSuchElementException;

import static edu.wisc.library.ocfl.core.util.FileTreeWalker.EventType.START_DIRECTORY;

/**
 * Iterator that iterates over OCFL object root directories. Object roots are identified by the presence of a file that's
 * prefixed with '0=ocfl_object'.
 */
class OcflObjectRoodDirIterator implements Iterator<Path>, Closeable {

    private static final String OCFL_OBJECT_MARKER_PREFIX = "0=ocfl_object";

    private Path start;
    private FileTreeWalker walker;
    private FileTreeWalker.Event next;
    private boolean started = false;

    OcflObjectRoodDirIterator(Path start) {
        this.start = Enforce.notNull(start, "start cannot be null");
        this.walker = new FileTreeWalker(EnumSet.noneOf(FileVisitOption.class), Integer.MAX_VALUE);
    }

    @Override
    public void close() {
        walker.close();
    }

    @Override
    public boolean hasNext() {
        if (!walker.isOpen()) {
            throw new IllegalStateException("FileTreeWalker is closed.");
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
        return result.file();
    }

    private void fetchNextIfNeeded() {
        try {
            if (next == null) {
                FileTreeWalker.Event event;
                if (!started) {
                    event = walker.walk(start);
                    started = true;
                } else {
                    event = walker.next();
                }

                while (event != null) {
                    if (event.ioeException() != null) {
                        throw event.ioeException();
                    }

                    var file = event.file();

                    if (event.type() == START_DIRECTORY) {
                        var children = Files.newDirectoryStream(file, p -> {
                            return p.getFileName().toString().startsWith(OCFL_OBJECT_MARKER_PREFIX);
                        });

                        // Found OCFL object marker -- current directory is an OCFL object root
                        if (children.iterator().hasNext()) {
                            // Do not process children
                            walker.pop();
                            next = event;
                            return;
                        }
                    }

                    event = walker.next();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

}
