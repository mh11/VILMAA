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

package vilmaa.genome.storage.hbase.allele.count.converter;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Test;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleCountPositionToAlleleCountHBaseProtoTest {
    AlleleCountPositionToAlleleCountHBaseProto to = new AlleleCountPositionToAlleleCountHBaseProto();
    AlleleCountHBaseProtoToAlleleCountPosition from = new AlleleCountHBaseProtoToAlleleCountPosition();

    @Test
    public void roundtripReference() throws Exception {
        AlleleCountPosition position = new AlleleCountPosition();
        List<Integer> value = Arrays.asList(3, 2, 1);
        position.getReference().put(1, value);
        position.getAlternate().put(2, value);
        position.getAltMap().computeIfAbsent("a", k -> new HashMap<>()).put(3, value);

        byte[] bytes = to.referenceToBytes(position);
        equalReference(position, from.referenceFromBytes(bytes));

        byte[] added = Bytes.add(bytes, bytes);
        List<Integer> addedValues = new ArrayList<>(value);
        addedValues.addAll(value); // double the values
        AlleleCountPosition addedPos = from.referenceFromBytes(added);
        assertEquals(addedValues, addedPos.getReference().get(1));
        assertTrue(addedPos.getAlternate().isEmpty());
        assertEquals(addedValues, addedPos.getAltMap().get("a").get(3));
    }

    @Test
    public void roundtripAlternate() throws Exception {
        AlleleCountPosition position = new AlleleCountPosition();
        List<Integer> value = Arrays.asList(3, 2, 1);
        position.getReference().put(1, value);
        position.getAlternate().put(2, value);
        position.getAltMap().computeIfAbsent("a", k -> new HashMap<>()).put(3, value);

        byte[] bytes = to.variantToBytes(position);
        equalAlternate(position, from.variantFromBytes(bytes));

        byte[] added = Bytes.add(bytes, bytes);
        List<Integer> addedValues = new ArrayList<>(value);
        addedValues.addAll(value); // double the values
        AlleleCountPosition addedPos = from.variantFromBytes(added);
        assertTrue(addedPos.getReference().isEmpty());
        assertTrue(addedPos.getAltMap().isEmpty());
        assertEquals(addedValues, addedPos.getAlternate().get(2));
    }

    public void equalReference(AlleleCountPosition expected, AlleleCountPosition target) {
        assertEquals(expected.getNotPass(), target.getNotPass());
        assertEquals(expected.getReference(), target.getReference());
        assertEquals(Collections.emptyMap(), target.getAlternate());
        assertEquals(expected.getAltMap(), target.getAltMap());

    }


    public void equalAlternate(AlleleCountPosition expected, AlleleCountPosition target) {
        assertEquals(Collections.emptyList(), target.getNotPass());
        assertEquals(Collections.emptyMap(), target.getReference());
        assertEquals(expected.getAlternate(), target.getAlternate());
        assertEquals(Collections.emptyMap(), target.getAltMap());

    }

}