package vilmaa.genome.storage.hbase;

import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 26/05/2017.
 */
public class VariantHbaseUtilTest {
    @Test
    public void inferType() throws Exception {
        assertEquals(VariantType.NO_VARIATION, VariantHbaseUtil.inferType("", ""));
        assertEquals(VariantType.NO_VARIATION, VariantHbaseUtil.inferType(".", "."));
        assertEquals(VariantType.NO_VARIATION, VariantHbaseUtil.inferType(".", ""));
        assertEquals(VariantType.NO_VARIATION, VariantHbaseUtil.inferType("", "."));

        assertEquals(VariantType.SNV, VariantHbaseUtil.inferType("A", "T"));

        assertEquals(VariantType.MNV, VariantHbaseUtil.inferType("AGG", "TCC"));

        assertEquals(VariantType.INSERTION, VariantHbaseUtil.inferType("", "T"));
        assertEquals(VariantType.INSERTION, VariantHbaseUtil.inferType(".", "T"));

        assertEquals(VariantType.DELETION, VariantHbaseUtil.inferType("A", ""));
        assertEquals(VariantType.DELETION, VariantHbaseUtil.inferType("A", "."));

        assertEquals(VariantType.MIXED, VariantHbaseUtil.inferType("AG", "T"));
        assertEquals(VariantType.MIXED, VariantHbaseUtil.inferType("A", "TC"));

    }

    @Test
    public void isInsertion() throws Exception {
        assertTrue(VariantHbaseUtil.isInsertion(".","A", VariantType.INSERTION));
        assertTrue(VariantHbaseUtil.isInsertion(".","A", VariantType.INDEL));
        assertFalse(VariantHbaseUtil.isInsertion("A",".", VariantType.INDEL));
    }


    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeRef() throws Exception {
        VariantHbaseUtil.inferType("<ABC>","T");
    }
    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeRefDot() throws Exception {
        VariantHbaseUtil.inferType(".A","T");
    }
    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeRefsquare() throws Exception {
        VariantHbaseUtil.inferType("[A","T");
    }
    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeAlt() throws Exception {
        VariantHbaseUtil.inferType("T","[T");
    }
    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeAltDot() throws Exception {
        VariantHbaseUtil.inferType("T",".T");
    }
    @Test(expected = UnsupportedOperationException.class)
    public void inferUnknownTypeAltSquare() throws Exception {
        VariantHbaseUtil.inferType("T","<T>");
    }

}