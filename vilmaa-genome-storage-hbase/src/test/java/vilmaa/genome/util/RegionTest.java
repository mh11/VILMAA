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

package vilmaa.genome.util;

import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 17/03/2017.
 */
public class RegionTest {
    @Test
    public void overlapPosition() throws Exception {
        assertFalse(new PointRegion("x",122).overlap(123));
        assertTrue(new PointRegion("x",123).overlap(123));
        assertFalse(new PointRegion("x",124).overlap(123));

        assertFalse(new PointRegion("x",123).overlap(122));
        assertFalse(new PointRegion("x",123).overlap(124));

        RegionImpl reg = new RegionImpl("x", 122, 124);
        assertFalse(reg.overlap(121));
        assertTrue(reg.overlap(122));
        assertTrue(reg.overlap(123));
        assertTrue(reg.overlap(124));
        assertFalse(reg.overlap(125));
    }

    @Test
    public void overlapRegion() throws Exception {
        RegionImpl query = new RegionImpl("x", 122, 125);
        assertFalse(new PointRegion("x",121).overlap(query));
        assertTrue(new PointRegion("x",122).overlap(query));
        assertTrue(new PointRegion("x",125).overlap(query));
        assertFalse(new PointRegion("x",126).overlap(query));


        assertFalse(new RegionImpl("x", 100, 121).overlap(query));
        assertTrue(new RegionImpl("x", 100, 122).overlap(query));
        assertTrue(new RegionImpl("x", 100, 200).overlap(query));
        assertTrue(new RegionImpl("x", 123, 124).overlap(query));
        assertTrue(new RegionImpl("x", 125, 200).overlap(query));
        assertFalse(new RegionImpl("x", 126, 200).overlap(query));
    }

    @Test
    public void overlapINSERTION() throws Exception {

        Region insertion = new RegionImpl("x", 100, 99);
        assertFalse(insertion.overlap(98));
        assertTrue(insertion.overlap(99));
        assertTrue(insertion.overlap(100));
        assertFalse(insertion.overlap(101));
    }

    @Test
    public void overlapStrictINSERTION() throws Exception {
        Region insertion = new RegionImpl("x", 100, 99);
        assertFalse(insertion.overlap(98, true));
        assertFalse(insertion.overlap(99, true));
        assertTrue(insertion.overlap(100, true));
        assertFalse(insertion.overlap(101, true));

        assertFalse(insertion.overlap(new RegionImpl("x", 97, 98), true));
        assertFalse(insertion.overlap(new RegionImpl("x", 97, 99), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 97, 100), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 98, 101), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 98, 100), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 99, 100), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 100, 100), true));
        assertTrue(insertion.overlap(new RegionImpl("x", 100, 101), true));
        assertFalse(insertion.overlap(new RegionImpl("x", 101, 102), true));

        assertTrue(insertion.overlap(insertion, true));
    }

    @Test
    public void coverRegion() throws Exception {
        Region a = new RegionImpl("x", 100, 200);
        assertFalse(new RegionImpl("x", 99, 201).coveredBy(a));
        assertFalse(new RegionImpl("x", 99, 200).coveredBy(a));
        assertFalse(new RegionImpl("x", 100, 201).coveredBy(a));
        assertTrue(new RegionImpl("x", 100, 200).coveredBy(a));
        assertTrue(new RegionImpl("x", 101, 199).coveredBy(a));

        assertFalse(a.coveredBy(new RegionImpl("x", 101, 199)));

    }

}