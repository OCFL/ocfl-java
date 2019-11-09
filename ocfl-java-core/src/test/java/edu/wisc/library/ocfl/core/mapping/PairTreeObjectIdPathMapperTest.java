package edu.wisc.library.ocfl.core.mapping;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PairTreeObjectIdPathMapperTest {

    private PairTreeObjectIdPathMapper mapper;

    @BeforeEach
    public void setup() {
        this.mapper = new PairTreeObjectIdPathMapper(new PairTreeEncoder(false), "obj", 4);
    }

    @Test
    public void rfcExamples() {
        assertEquals("ar/k+/12/34/5=/6/45=6", mapper.map("ark:12345/6"));
        assertEquals("ar/k+/=1/30/30/=x/t1/2t/3/12t3", mapper.map("ark:/13030/xt12t3"));
        assertEquals("ht/tp/+=/=n/2t/,i/nf/o=/ur/n+/nb/n+/se/+k/b+/re/po/s-/1/os-1", mapper.map("http://n2t.info/urn:nbn:se:kb:repos-1"));
        assertEquals("wh/at/-t/he/-^/2a/@^/3f/#!/^5/e!/^3/f/!^3f", mapper.map("what-the-*@?#!^!?"));
    }

    @Test
    public void shouldUseDefaultEncapsulationStringWhenIdLessThan3Chars() {
        assertEquals("o1/obj", mapper.map("o1").toString());
        assertEquals("1/obj", mapper.map("1").toString());
    }

    @Test
    public void shouldUseEntireIdWhenMoreThan2CharsButLessThanEncapsulationSubstringLength() {
        assertEquals("ab/c/abc", mapper.map("abc").toString());
    }

    @Test
    public void shouldRejectIdsWithATrailingDot() {
        mapper = new PairTreeObjectIdPathMapper(new UrlEncoder(false), "obj", 4);
        assertThrows(IllegalArgumentException.class, () -> mapper.map("ab."));
    }

    @Test
    public void shouldRejectIdsWithDoubleDot() {
        mapper = new PairTreeObjectIdPathMapper(new UrlEncoder(false), "obj", 4);
        assertThrows(IllegalArgumentException.class, () -> mapper.map("ab..cd"));
    }

}
