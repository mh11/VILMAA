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