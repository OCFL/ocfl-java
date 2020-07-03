package edu.wisc.library.ocfl.core.path.mapper;

import edu.wisc.library.ocfl.core.util.PercentEscaper;

/**
 * This class defines common {@link LogicalPathMapper} implementations.
 */
public final class LogicalPathMappers {

    private static final char ASCII_CTRL_START = 0;
    private static final char ASCII_CTRL_END = 31;
    private static final char ASCII_CTRL_EXT_START = 127;
    private static final char ASCII_CTRL_EXT_END = 160;

    private static final String ENCODE_WINDOWS = "<>:\"\\|?* ";
    private static final String ENCODE_LINUX = " ";
    private static final String ENCODE_CLOUD = "\\#[]*? ";
    private static final String ENCODE_ALL = "<>:\"\\|?* #[]";

    private LogicalPathMappers() {

    }

    /**
     * Creates a {@link LogicalPathMapper} that directly maps logical paths to content paths, making no changes.
     *
     * @return mapper
     */
    public static LogicalPathMapper directMapper() {
        return new DirectLogicalPathMapper();
    }

    /**
     * Creates a percent-encoding mapper that encodes the following characters that are problematic for Windows:
     *
     * <ul>
     *     <li>ASCII characters codes: x0-x1f</li>
     *     <li>ASCII characters codes: x7f-xa0</li>
     *     <li>Characters: &lt;&gt;:"\|?* </li>
     * </ul>
     *
     * This should produce generally safe content paths, but they are not guaranteed safe.
     *
     * @return mapper
     */
    public static LogicalPathMapper percentEncodingWindowsMapper() {
        return new PercentEncodingLogicalPathMapper(PercentEscaper.builder()
                .useUppercase(false)
                .plusForSpace(false)
                .addUnsafeCharRange(ASCII_CTRL_START, ASCII_CTRL_END)
                .addUnsafeCharRange(ASCII_CTRL_EXT_START, ASCII_CTRL_EXT_END)
                .addUnsafeChars(ENCODE_WINDOWS)
                .build());
    }

    /**
     * Creates a percent-encoding that encodes the following characters:
     *
     * <ul>
     *     <li>ASCII characters codes: x0-x1f</li>
     *     <li>ASCII characters codes: x7f-xa0</li>
     *     <li>Characters:  </li>
     * </ul>
     *
     * Linux is extremely permissive when it comes to the characters that are allowed in filenames. Some of the characters
     * encoded here are in fact legal. However, it is often undesirable for them to appear in filenames.
     *
     * @return mapper
     */
    public static LogicalPathMapper percentEncodingLinuxMapper() {
        return new PercentEncodingLogicalPathMapper(PercentEscaper.builder()
                .useUppercase(false)
                .plusForSpace(false)
                .addUnsafeCharRange(ASCII_CTRL_START, ASCII_CTRL_END)
                .addUnsafeCharRange(ASCII_CTRL_EXT_START, ASCII_CTRL_EXT_END)
                .addUnsafeChars(ENCODE_LINUX)
                .build());
    }

    /**
     * Creates a percent-encoding that encodes the following characters that are problematic for some cloud storage platforms:
     *
     * <ul>
     *     <li>ASCII characters codes: x0-x1f</li>
     *     <li>ASCII characters codes: x7f-xa0</li>
     *     <li>Characters: \#[]*? </li>
     * </ul>
     *
     * This should produce generally safe content paths, but they are not guaranteed safe.
     *
     * @return mapper
     */
    public static LogicalPathMapper percentEncodingCloudMapper() {
        return new PercentEncodingLogicalPathMapper(PercentEscaper.builder()
                .useUppercase(false)
                .plusForSpace(false)
                .addUnsafeCharRange(ASCII_CTRL_START, ASCII_CTRL_END)
                .addUnsafeCharRange(ASCII_CTRL_EXT_START, ASCII_CTRL_EXT_END)
                .addUnsafeChars(ENCODE_CLOUD)
                .build());
    }

    /**
     * Creates a percent-encoding that encodes the following characters that are generally problematic across Windows, linux,
     * and cloud platforms:
     *
     * <ul>
     *     <li>ASCII characters codes: x0-x1f</li>
     *     <li>ASCII characters codes: x7f-xa0</li>
     *     <li>Characters: &lt;&gt;:"\|?* #[]</li>
     * </ul>
     *
     * This should produce generally safe content paths, but they are not guaranteed safe.
     *
     * @return mapper
     */
    public static LogicalPathMapper percentEncodingAllMapper() {
        return new PercentEncodingLogicalPathMapper(PercentEscaper.builder()
                .useUppercase(false)
                .plusForSpace(false)
                .addUnsafeCharRange(ASCII_CTRL_START, ASCII_CTRL_END)
                .addUnsafeCharRange(ASCII_CTRL_EXT_START, ASCII_CTRL_EXT_END)
                .addUnsafeChars(ENCODE_ALL)
                .build());
    }

    /**
     * Creates a percent-encoding that percent-encodes every character except: [a-zA-Z0-9_-].
     * <p>
     * This is an extremely conservative mapper that will produce content paths without problematic characters. However,
     * it will also greatly inflate the size of the paths, potentially making them longer than the filesystem's filename
     * size limit.
     *
     * @return mapper
     */
    public static LogicalPathMapper conservativePercentEncodingMapper() {
        return new PercentEncodingLogicalPathMapper(PercentEscaper.builderWithSafeAlphaNumeric()
                .useUppercase(false)
                .plusForSpace(false)
                .addSafeChars("-_")
                .build());
    }

}
