package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.core.extension.layout.config.*;
import edu.wisc.library.ocfl.core.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class ObjectIdPathMapperBuilderTest {

    private ObjectIdPathMapperBuilder builder;

    @BeforeEach
    public void setup() {
        this.builder = new ObjectIdPathMapperBuilder();
    }

    @Test
    public void shouldFailFlatCreationWhenNoEncodingDefined() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "encoding cannot be null", () -> {
            builder.build(new FlatLayoutConfig());
        });
    }

    @Test
    public void shouldFailFlatCreationWhenCaseNotSpecified() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "casing cannot be null", () -> {
            builder.build(new FlatLayoutConfig().setEncoding(EncodingType.URL));
        });
    }

    @Test
    public void shouldFailFlatCreationWhenAlgorithmNotSpecified() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "digestAlgorithm cannot be null", () -> {
            builder.build(new FlatLayoutConfig().setEncoding(EncodingType.HASH).setCasing(Casing.LOWER));
        });
    }


    @Test
    public void shouldFailTupleCreationWhenNoEncodingDefined() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "encoding cannot be null", () -> {
            builder.build(new NTupleLayoutConfig());
        });
    }

    @Test
    public void shouldFailTupleCreationWhenCaseNotSpecified() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "casing cannot be null", () -> {
            builder.build(new NTupleLayoutConfig().setEncoding(EncodingType.URL));
        });
    }

    @Test
    public void shouldFailTupleCreationWhenAlgorithmNotSpecified() {
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "digestAlgorithm cannot be null", () -> {
            builder.build(new NTupleLayoutConfig().setEncoding(EncodingType.HASH).setCasing(Casing.LOWER));
        });
    }

    @Test
    public void shouldFailTupleCreationWhenSubstringWithNoSize() {
        var config = DefaultLayoutConfig.pairTreeConfig();
        config.getEncapsulation().setSubstringSize(null);
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "substring size cannot be null", () -> {
            builder.build(config);
        });
    }

    @Test
    public void shouldFailTupleCreationWhenIdEncapWithNoEncoding() {
        var config = DefaultLayoutConfig.nTupleHashConfig();
        config.getEncapsulation().setEncoding(null);
        OcflAsserts.assertThrowsWithMessage(NullPointerException.class, "encoding cannot be null", () -> {
            builder.build(config);
        });
    }

    @Test
    public void shouldFailTupleCreationWhenDepthNonZeroAndSubstringSet() {
        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "Encapsulation substrings may only be used when depth is unbound", () -> {
            builder.build(DefaultLayoutConfig.pairTreeConfig().setDepth(2));
        });
    }

    @Test
    public void shouldFailTupleCreationWhenSubstringShorterThanTupleSize() {
        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "n-tuple segment size must be less than the encapsulation substring size", () -> {
            builder.build(DefaultLayoutConfig.pairTreeConfig().setSize(4));
        });
    }

    @Test
    public void shouldFailTupleCreationWhenSizeZero() {
        var config = DefaultLayoutConfig.nTupleHashConfig();
        config.setSize(0);
        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "size must be greater than 0", () -> {
            builder.build(config);
        });
    }

    @Test
    public void shouldFailTupleCreationWhenDefaultEncapNotSet() {
        var config = DefaultLayoutConfig.nTupleHashConfig();
        config.getEncapsulation().setDefaultString(null);
        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "defaultEncapsulation cannot be blank", () -> {
            builder.build(config);
        });
    }

    @Test
    public void shouldFailTupleCreationWhenDefaultEncapTooShort() {
        var config = DefaultLayoutConfig.nTupleHashConfig();
        config.getEncapsulation().setDefaultString("o");
        OcflAsserts.assertThrowsWithMessage(IllegalArgumentException.class, "defaultEncapsulation string must be longer than the segment size", () -> {
            builder.build(config);
        });
    }

}
