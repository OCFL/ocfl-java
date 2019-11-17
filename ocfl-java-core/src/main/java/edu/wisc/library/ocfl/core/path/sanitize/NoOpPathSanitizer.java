package edu.wisc.library.ocfl.core.path.sanitize;

/**
 * PathSanitizer that simply returns the logical path without changing anything
 */
public class NoOpPathSanitizer implements PathSanitizer {

    @Override
    public String sanitize(String logicalPath) {
        return logicalPath;
    }

}
