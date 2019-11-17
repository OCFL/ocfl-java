package edu.wisc.library.ocfl.core.path.sanitize;

/**
 * PathSanitizers are used to clean logical paths so that they can be safely mapped to content paths for storage. In the
 * most extreme case, this may involve completely transforming the path. The primary concerns are transforming illegal
 * characters and ensuring that the path is not too long.
 */
public interface PathSanitizer {

    /**
     * Sanitizes the given logical path to a storage safe path
     *
     * @param logicalPath path to sanitize
     * @return sanitized path that can be mapped to a content path
     */
    String sanitize(String logicalPath);

}
