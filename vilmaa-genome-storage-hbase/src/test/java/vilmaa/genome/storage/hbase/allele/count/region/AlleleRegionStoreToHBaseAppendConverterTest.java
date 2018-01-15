package vilmaa.genome.storage.hbase.allele.count.region;

import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.util.Region;
import vilmaa.genome.util.RegionImpl;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Result;
import org.junit.Test;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.util.*;
import java.util.stream.Collectors;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * Test a roundtrip of persisting and loading alleles.
 * Created by mh719 on 22/03/2017.
 */
public class AlleleRegionStoreToHBaseAppendConverterTest {

    protected static final byte[] COLUMN_FAMILY = {1};
    protected GenomeHelper gh = new GenomeHelper(new Configuration());
    protected AlleleRegionStoreToHBaseAppendConverter from = new AlleleRegionStoreToHBaseAppendConverter(COLUMN_FAMILY, 1);
    protected HBaseToAlleleRegionStoreConverter to = new HBaseToAlleleRegionStoreConverter
            (COLUMN_FAMILY, 1, from.getRegionSize(), b -> gh.extractVariantFromVariantRowKey(b));

    @Test
    public void convertSnpMnv() throws Exception {
        testSnpMnv(9, 1, false);
        testSnpMnv(9, 2, true);
        testSnpMnv(10, 1, true);
        testSnpMnv(13, 6, true);
        testSnpMnv(19, 1, true);
        testSnpMnv(20, 1, false);
    }

    private void testSnpMnv(int start, int length, boolean included) {
        int end = start + length - 1;
        from.setRegionSize(2);
        to.setRegionSize(2);
        AlleleRegionStore store = new AlleleRegionStore(10, 19);
        VariantType vtype = length == 1? VariantType.SNV : VariantType.MNV;
        String[] alleles = {StringUtils.repeat('A', length ), StringUtils.repeat('T', length )};
        AlleleInfo info = new AlleleInfo(1, 123, 22, alleles, vtype, true);
        RegionImpl<AlleleInfo> region = new RegionImpl<>(info, start, end);
        store.add(region);

        AlleleRegionStore newStore = process(store);

        List<Region<AlleleInfo>> refA = store.getVariation(store.getTargetRegion());
        List<Region<AlleleInfo>> refB = newStore.getVariation(newStore.getTargetRegion());

        if (!included) {
            assertEquals(0, refA.size());
            assertEquals(0, refB.size());
            return;
        }
        assertEquals(1, refA.size());
        assertEquals(1, refB.size()); // spit due to 100bp region overlap

        refB.sort(Comparator.comparingInt(Region::getStart));
        assertEquals(start, refB.get(0).getStart());
        assertEquals(end, refB.get(0).getEnd());

        refB.forEach(r -> assertEquals(1, r.getData().getCount()));
        refB.forEach(r -> assertEquals(123, r.getData().getDepth()));
        refB.forEach(r -> assertEquals(new HashSet<>(Arrays.asList(22)), r.getData().getSampleIds()));
        refB.forEach(r -> assertArrayEquals(alleles, r.getData().getId()));
        refB.forEach(r -> assertEquals(vtype, r.getData().getType()));
    }

    @Test
    public void convertInsertion() throws Exception {
        testInsertion(9, 3, false);
        testInsertion(10, 3, true);
        testInsertion(12, 3, true);
        testInsertion(15, 3, true);
        testInsertion(19, 3, true);
        testInsertion(20, 3, false);
        testInsertion(21, 3, false);
    }

    private void testInsertion(int start, int length, boolean included) {
        int end = start - 1;
        from.setRegionSize(2);
        to.setRegionSize(2);
        AlleleRegionStore store = new AlleleRegionStore(10, 19);
        VariantType vtype = VariantType.INSERTION;
        String[] alleles = {"", StringUtils.repeat('A', length )};
        AlleleInfo info = new AlleleInfo(1, 123, 22, alleles, vtype, true);
        RegionImpl<AlleleInfo> region = new RegionImpl<>(info, start, end);
        store.add(region);

        AlleleRegionStore newStore = process(store);

        List<Region<AlleleInfo>> refA = store.getVariation(store.getTargetRegion());
        List<Region<AlleleInfo>> refB = newStore.getVariation(newStore.getTargetRegion());

        if (!included) {
            assertEquals(0, countVariants(refA));
            assertEquals(0, countVariants(refB));
            return;
        }
        assertEquals(1, countVariants(refA));
        assertEquals(1, countVariants(refB)); // spit due to 100bp region overlap

        refB.sort(Comparator.comparingInt(Region::getStart));
        assertEquals(start, refB.get(0).getStart());
        assertEquals(end, refB.get(0).getEnd());

        refB.forEach(r -> assertEquals(1, r.getData().getCount()));
        refB.forEach(r -> assertEquals(123, r.getData().getDepth()));
        refB.forEach(r -> assertEquals(new HashSet<>(Arrays.asList(22)), r.getData().getSampleIds()));
        refB.forEach(r -> assertArrayEquals(alleles, r.getData().getId()));
        refB.forEach(r -> assertEquals(vtype, r.getData().getType()));
    }

    private int countVariants(List<Region<AlleleInfo>> refB) {
        return refB.stream()
                .map(x -> x.getStart() + "_" + x.getEnd() + "_" + x.getData().getDepth() + "_" + x.getData().getIdString())
                .collect(Collectors.toSet()).size();
    }

    @Test
    public void convertDeletion() throws Exception {

        testDeletion(5, 9, false);
        testDeletion(5, 10);
        testDeletion(9, 12);
        testDeletion(9, 21);
        testDeletion(10, 22);
        testDeletion(10, 17);
        testDeletion(10, 19);
        testDeletion(17, 19);
        testDeletion(18, 22);
        testDeletion(19, 22, true);
    }

    private void testDeletion(int delStart, int delEnd) {
        testDeletion(delStart, delEnd, true);
    }
    private void testDeletion(int delStart, int delEnd, boolean included) {
        from.setRegionSize(2);
        to.setRegionSize(2);
        AlleleRegionStore store = new AlleleRegionStore(10, 19);
        VariantType vtype = VariantType.DELETION;
        String[] alleles = {StringUtils.repeat('A', delEnd - delStart + 1),""};
        AlleleInfo info = new AlleleInfo(1, 123, 22, alleles, vtype, true);
        RegionImpl<AlleleInfo> region = new RegionImpl<>(info, delStart, delEnd);
        store.add(region);

        AlleleRegionStore newStore = process(store);

        List<Region<AlleleInfo>> refA = store.getVariation(store.getTargetRegion());
        List<Region<AlleleInfo>> refB = newStore.getVariation(newStore.getTargetRegion());

        if (!included) {
            assertEquals(0, refA.size());
            assertEquals(0, refB.size());
            return;
        }
        assertEquals(1, refA.size());
        assertEquals(1, refB.size()); // spit due to 100bp region overlap

        refB.sort(Comparator.comparingInt(Region::getStart));
        assertEquals(delStart, refB.get(0).getStart());
        assertEquals(delEnd, refB.get(0).getEnd());

        refB.forEach(r -> assertEquals(1, r.getData().getCount()));
        refB.forEach(r -> assertEquals(123, r.getData().getDepth()));
        refB.forEach(r -> assertEquals(new HashSet<>(Arrays.asList(22)), r.getData().getSampleIds()));
        refB.forEach(r -> assertArrayEquals(alleles, r.getData().getId()));
        refB.forEach(r -> assertEquals(vtype, r.getData().getType()));
    }

    @Test
    public void convertRefAndNoCall() throws Exception {
        testRegionType(AlleleInfo.getReferenceAllele());
        testRegionType(AlleleInfo.getNoCallAllele());
    }

    public void testRegionType(String[] alleles) {

        AlleleRegionStore store = new AlleleRegionStore(1000, 1999);
        VariantType vtype = VariantType.NO_VARIATION;
        AlleleInfo info = new AlleleInfo(1, 123, 22, alleles, vtype, true);
        RegionImpl<AlleleInfo> region = new RegionImpl<>(info, 1090, 1110);
        store.add(region);

        AlleleRegionStore newStore = process(store);

        List<Region<AlleleInfo>> refA = store.getReference(store.getTargetRegion());
        List<Region<AlleleInfo>> refB = newStore.getReference(newStore.getTargetRegion());
        if (alleles.length == 1) {
            refA = store.getNocall(store.getTargetRegion());
            refB = newStore.getNocall(store.getTargetRegion());
        }

        assertEquals(1, refA.size());
        assertEquals(2, refB.size()); // spit due to 100bp region overlap

        refB.sort(Comparator.comparingInt(Region::getStart));
        assertEquals(1090, refB.get(0).getStart());
        assertEquals(1099, refB.get(0).getEnd());
        assertEquals(1100, refB.get(1).getStart());
        assertEquals(1110, refB.get(1).getEnd());

        refB.forEach(r -> assertEquals(1, r.getData().getCount()));
        refB.forEach(r -> assertEquals(123, r.getData().getDepth()));
        refB.forEach(r -> assertEquals(new HashSet<>(Arrays.asList(22)), r.getData().getSampleIds()));
        refB.forEach(r -> assertArrayEquals(alleles, r.getData().getId()));
        refB.forEach(r -> assertEquals(vtype, r.getData().getType()));
    }

    @Test
    public void convertRefAndNoCallPos() throws Exception {
        from.setRegionSize(2);
        to.setRegionSize(2);
        testRegionTypePos(5, 22, AlleleInfo.getReferenceAllele());
        testRegionTypePos(10, 20, AlleleInfo.getReferenceAllele());
        testRegionTypePos(12, 17, AlleleInfo.getReferenceAllele());


        testRegionTypePos(5, 22, AlleleInfo.getNoCallAllele());
        testRegionTypePos(10, 20, AlleleInfo.getNoCallAllele());
        testRegionTypePos(12, 17, AlleleInfo.getNoCallAllele());


        testRegionTypePos(10, 10, AlleleInfo.getReferenceAllele());
        testRegionTypePos(10, 10, AlleleInfo.getNoCallAllele());
    }

    public void testRegionTypePos(int start, int end, String[] alleles) {

        AlleleRegionStore store = new AlleleRegionStore(10, 19);
        VariantType vtype = VariantType.NO_VARIATION;
        AlleleInfo info = new AlleleInfo(1, 123, 22, alleles, vtype, true);
        RegionImpl<AlleleInfo> region = new RegionImpl<>(info, start, end);
        store.add(region);

        start = Math.max(10, start);
        end = Math.min(19, end);

        AlleleRegionStore newStore = process(store);

        List<Region<AlleleInfo>> refA = store.getReference(store.getTargetRegion());
        List<Region<AlleleInfo>> refB = newStore.getReference(newStore.getTargetRegion());
        if (alleles.length == 1) {
            refA = store.getNocall(store.getTargetRegion());
            refB = newStore.getNocall(store.getTargetRegion());
        }

        assertEquals(1, refA.size());
//        assertEquals(2, refB.size()); // spit due to 100bp region overlap

        refB.sort(Comparator.comparingInt(Region::getStart));
        assertEquals(start, refB.get(0).getStart());
        assertEquals(end, refB.get(refB.size() - 1).getEnd());

        refB.forEach(r -> assertEquals(1, r.getData().getCount()));
        refB.forEach(r -> assertEquals(123, r.getData().getDepth()));
        refB.forEach(r -> assertEquals(new HashSet<>(Arrays.asList(22)), r.getData().getSampleIds()));
        refB.forEach(r -> assertArrayEquals(alleles, r.getData().getId()));
        refB.forEach(r -> assertEquals(vtype, r.getData().getType()));
    }

    private AlleleRegionStore process(AlleleRegionStore store) {
        AlleleRegionStore newStore = new AlleleRegionStore(store.getTargetRegion());
        Collection<Append> x = from.convert("x", store);
        x.forEach( a -> to.convert(newStore, Result.create(a.getFamilyCellMap().get(COLUMN_FAMILY))));
        return newStore;
    }

}