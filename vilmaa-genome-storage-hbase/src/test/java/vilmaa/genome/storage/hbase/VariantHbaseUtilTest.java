/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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