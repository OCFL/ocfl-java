package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.util.Enforce;
import edu.wisc.library.ocfl.core.encode.*;
import edu.wisc.library.ocfl.core.extension.layout.config.*;

/**
 * Constructs an {@link ObjectIdPathMapper} based on {@link LayoutConfig}.
 */
public class ObjectIdPathMapperBuilder {

    /**
     * Returns a {@link ObjectIdPathMapper} for the {@link LayoutConfig}.
     *
     * @see DefaultLayoutConfig
     *
     * @param layoutConfig configuration
     * @return mapper
     */
    public ObjectIdPathMapper build(LayoutConfig layoutConfig) {
        Enforce.notNull(layoutConfig, "layoutConfig cannot be null");

        ObjectIdPathMapper mapper;

        if (layoutConfig instanceof NTupleLayoutConfig) {
            mapper = buildNTupleMapper((NTupleLayoutConfig) layoutConfig);
        } else if (layoutConfig instanceof FlatLayoutConfig) {
            mapper = buildFlatMapper((FlatLayoutConfig) layoutConfig);
        } else {
            throw new IllegalStateException("Unknown layout config: " + layoutConfig);
        }

        return mapper;
    }

    private ObjectIdPathMapper buildFlatMapper(FlatLayoutConfig config) {
        var encoder = buildEncoder(config.getEncoding(), config);
        return new FlatObjectIdPathMapper(encoder);
    }

    private ObjectIdPathMapper buildNTupleMapper(NTupleLayoutConfig config) {
        var encoder = buildEncoder(config.getEncoding(), config);
        var encapsulator = buildEncapsulator(config, encoder);

        return new NTupleObjectIdPathMapper(encoder, encapsulator,
                config.getSize(), config.getDepth(),
                config.getEncapsulation().getDefaultString());
    }

    private Encoder buildEncoder(EncodingType encodingType, LayoutConfig config) {
        Enforce.notNull(encodingType, "encoding cannot be null");

        var useUpper = false;

        if (encodingType != EncodingType.NONE) {
            Enforce.notNull(config.getCasing(), "casing cannot be null");
            useUpper = config.getCasing() == Casing.UPPER;
        }

        switch (encodingType) {
            case NONE:
                return new NoOpEncoder();
            case URL:
                return new UrlEncoder(useUpper);
            case PAIRTREE:
                return new PairTreeEncoder(useUpper);
            case HASH:
                return new DigestEncoder(config.getDigestAlgorithm(), useUpper);
            default:
                throw new IllegalArgumentException("Unmapped encoding: " + encodingType);
        }
    }

    private Encapsulator buildEncapsulator(NTupleLayoutConfig config, Encoder idEncoder) {
        var encapConfig = Enforce.notNull(config.getEncapsulation(), "encapsulation config cannot be null");

        Enforce.notNull(encapConfig.getType(), "encapsulation type cannot be null");

        switch (encapConfig.getType()) {
            case ID:
                var useEncoded = config.getEncoding() == encapConfig.getEncoding();
                if (useEncoded) {
                    return IdEncapsulator.useEncodedId();
                }
                return IdEncapsulator.useOriginalId(buildEncoder(encapConfig.getEncoding(), config));
            case SUBSTRING:
                if (config.getDepth() != 0) {
                    throw new IllegalArgumentException("Encapsulation substrings may only be used when depth is unbound (set to 0)");
                }
                Enforce.notNull(encapConfig.getSubstringSize(), "substring size cannot be null");
                Enforce.expressionTrue(config.getSize() < encapConfig.getSubstringSize(), config.getSize(),
                        "n-tuple segment size must be less than the encapsulation substring size");

                return new SubstringEncapsulator(encapConfig.getSubstringSize());
            default:
                throw new IllegalArgumentException("Unmapped encapsulation type: " + encapConfig.getType());
        }
    }

}
