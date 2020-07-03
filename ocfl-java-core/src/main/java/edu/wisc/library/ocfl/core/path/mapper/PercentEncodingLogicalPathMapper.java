package edu.wisc.library.ocfl.core.path.mapper;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.util.PercentEscaper;

/**
 * A {@link LogicalPathMapper} that percent-encodes specific characters in logical paths to produce safe content paths.
 */
public class PercentEncodingLogicalPathMapper implements LogicalPathMapper {

    private final PercentEscaper percentEscaper;

    /**
     * Constructs a new {@link PercentEncodingLogicalPathMapper}.
     *
     * @see LogicalPathMappers#percentEncodingWindowsMapper()
     * @see LogicalPathMappers#percentEncodingLinuxMapper()
     * @see LogicalPathMappers#percentEncodingCloudMapper()
     * @see LogicalPathMappers#percentEncodingAllMapper()
     * @see LogicalPathMappers#conservativePercentEncodingMapper()
     *
     * @param percentEscaper the escaper to use for percent-encoding
     */
    public PercentEncodingLogicalPathMapper(PercentEscaper percentEscaper) {
        this.percentEscaper = Enforce.notNull(percentEscaper, "percentEscaper cannot be null");
    }

    @Override
    public String toContentPathPart(String logicalPath) {
        return percentEscaper.escape(logicalPath);
    }

}
