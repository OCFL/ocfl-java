package edu.wisc.library.ocfl.core.path.constraint;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;

import java.nio.file.FileSystems;

/**
 * Windows only: Rejects filenames that contain backslashes
 */
public class BackslashPathSeparatorConstraint implements PathCharConstraint {

    private char pathSeparator;
    private boolean isBackslash;

    public BackslashPathSeparatorConstraint() {
        // TODO note: this not 100% accurate because the filesystem that the repository is on may be different than the
        //            default filesystem
        pathSeparator = FileSystems.getDefault().getSeparator().charAt(0);
        isBackslash = pathSeparator == '\\';
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void apply(char c, String path) {
        if (isBackslash && c == '\\') {
            throw new PathConstraintException(
                    String.format("The path contains a \\ character in a filename, which is illegal on the local filesystem.", path));
        }
    }

}
