package edu.wisc.library.ocfl.core.mapping;

import edu.wisc.library.ocfl.api.exception.PathConstraintException;
import edu.wisc.library.ocfl.api.model.DigestAlgorithm;
import edu.wisc.library.ocfl.core.encode.DigestEncoder;
import edu.wisc.library.ocfl.core.encode.NoOpEncoder;
import edu.wisc.library.ocfl.core.encode.PairTreeEncoder;
import edu.wisc.library.ocfl.core.encode.UrlEncoder;
import edu.wisc.library.ocfl.test.OcflAsserts;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class NTupleObjectIdPathMapperTest {

    private NTupleObjectIdPathMapper pairTreeMapper;
    private String objectId = "http://library.wisc.edu/123";

    @BeforeEach
    public void setup() {
        this.pairTreeMapper = new NTupleObjectIdPathMapper(new PairTreeEncoder(false),
                new SubstringEncapsulator(4),
                2, 0, "obj");
    }

    @Test
    public void rfcExamples() {
        assertEquals("ar/k+/12/34/5=/6/45=6", pairTreeMapper.map("ark:12345/6"));
        assertEquals("ar/k+/=1/30/30/=x/t1/2t/3/12t3", pairTreeMapper.map("ark:/13030/xt12t3"));
        assertEquals("ht/tp/+=/=n/2t/,i/nf/o=/ur/n+/nb/n+/se/+k/b+/re/po/s-/1/os-1", pairTreeMapper.map("http://n2t.info/urn:nbn:se:kb:repos-1"));
        assertEquals("wh/at/-t/he/-^/2a/@^/3f/#!/^5/e!/^3/f/!^3f", pairTreeMapper.map("what-the-*@?#!^!?"));
    }

    @Test
    public void shouldUseDefaultEncapsulationStringWhenIdLessThan3Chars() {
        assertEquals("o1/obj", pairTreeMapper.map("o1"));
        assertEquals("1/obj", pairTreeMapper.map("1"));
    }

    @Test
    public void shouldUseEntireIdWhenMoreThan2CharsButLessThanEncapsulationSubstringLength() {
        assertEquals("ab/c/abc", pairTreeMapper.map("abc"));
    }

    @Test
    public void shouldMap3LevelsDeepWith2CharDirNames() {
        var mapper = new NTupleObjectIdPathMapper(new DigestEncoder(DigestAlgorithm.sha256, false),
                IdEncapsulator.useEncodedId(), 2, 2, "obj");
        var result = mapper.map(objectId);
        assertEquals("ed/75/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

    @Test
    public void shouldUseOriginalIdAsEncapsulation() {
        var mapper = new NTupleObjectIdPathMapper(new DigestEncoder(DigestAlgorithm.sha256, false),
                IdEncapsulator.useOriginalId(new UrlEncoder(false)), 2, 2, "obj");
        var result = mapper.map(objectId);
        assertEquals("ed/75/http%3a%2f%2flibrary.wisc.edu%2f123", result);
    }

    @Test
    public void shouldMap5LevelsDeepWith2CharDirNames() {
        var mapper = new NTupleObjectIdPathMapper(new DigestEncoder(DigestAlgorithm.sha256, false),
                IdEncapsulator.useEncodedId(), 2, 4, "obj");
        var result = mapper.map(objectId);
        assertEquals("ed/75/58/5a/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

    @Test
    public void shouldMap2LevelsDeepWith3CharDirNames() {
        var mapper = new NTupleObjectIdPathMapper(new DigestEncoder(DigestAlgorithm.sha256, false),
                IdEncapsulator.useEncodedId(), 3, 2, "objLong");
        var result = mapper.map(objectId);
        assertEquals("ed7/558/ed75585a6e8deef3b3f620e5f6d0999908c09e92cad5e112aa5eac5507000d8b", result);
    }

    @Test
    public void shouldRejectEncodedPathsWithDotFilenames() {
        var mapper = new NTupleObjectIdPathMapper(new UrlEncoder(false),
                IdEncapsulator.useEncodedId(), 2, 2, "obj");
        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "The filename '..' contains an invalid sequence ^\\.{1,2}$. Path: 12/../12..34", () -> {
            mapper.map("12..34");
        });
    }

    @Test
    public void shouldRejectEncodedPathsWithEmpty() {
        var mapper = new NTupleObjectIdPathMapper(new NoOpEncoder(),
                IdEncapsulator.useEncodedId(), 2, 2, "obj");
        OcflAsserts.assertThrowsWithMessage(PathConstraintException.class, "The path contains an illegal empty filename. Path: 12////12//34", () -> {
            mapper.map("12//34");
        });
    }

}
