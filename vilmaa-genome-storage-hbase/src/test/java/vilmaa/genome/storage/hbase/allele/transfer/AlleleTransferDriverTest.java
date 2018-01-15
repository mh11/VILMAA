package vilmaa.genome.storage.hbase.allele.transfer;

import vilmaa.genome.storage.hbase.allele.AlleleTransferDriver;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Created by mh719 on 01/02/2017.
 */
public class AlleleTransferDriverTest {
    @Test
    public void parseRegion() throws Exception {

        Pair<String, Pair<Integer, Integer>> region = AlleleTransferDriver.parseRegion("10:123-432");
        assertNotNull(region);
        assertEquals("10", region.getLeft());
        assertNotNull(region.getRight());
        assertEquals(123, region.getRight().getLeft().intValue());
        assertEquals(432, region.getRight().getRight().intValue());

    }

}