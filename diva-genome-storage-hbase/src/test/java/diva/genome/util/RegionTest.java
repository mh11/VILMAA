package diva.genome.util;

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
        assertFalse(insertion.overlap(99));
        assertTrue(insertion.overlap(100));
        assertFalse(insertion.overlap(101));

    }


}