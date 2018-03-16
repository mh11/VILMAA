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

package vilmaa.genome.storage.hbase.allele.count.position;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.AlleleInfo;
import vilmaa.genome.util.RegionImpl;
import org.apache.commons.collections.map.HashedMap;
import org.junit.Test;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantType;

import java.util.*;

import static org.junit.Assert.*;

/**
 * Created by mh719 on 25/01/2017.
 */
public class HBaseAlleleCalculatorTest {

    public static Variant getVariant(String var, String studyId, String sample, String gt, Map<String, String> fileValues) {
        return getVariant(var, studyId, 1, sample, gt, fileValues);
    }

    public static Variant getVariant(String var, String studyId, Integer fileId,  String sample, String gt, Map<String, String> fileValues) {
        Variant b = new Variant(var);
        StudyEntry sb = new StudyEntry(studyId, studyId);
        sb.setFiles(Collections.singletonList(new FileEntry(fileId + "", "1", new HashedMap())));
        b.setStudies(Collections.singletonList(sb));
        sb.setFormat(Arrays.asList("GT"));
        Map<String, Integer> samplePos = new HashMap<>();
        samplePos.put(sample, 0);
        sb.setSamplesPosition(samplePos);
        sb.setSamplesData(Arrays.asList(Arrays.asList(gt)));

        FileEntry file = new FileEntry(fileId + "",gt, fileValues);
        sb.setFiles(Collections.singletonList(file));
        return b;
    }


        @Test
    public void calculateInsertion() throws Exception {
        String studyId = "22";
        Map<String, Integer> idMapping = new HashMap<>();
        String sampleName1 = "Sample 1";
        idMapping.put(sampleName1, 1);
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator(studyId, idMapping);

        Variant variant = getVariant("1:100:-:AT", studyId, sampleName1, "0/1",  map("FILTER","not-pass"));
//        variant.getStudy(studyId).setSecondaryAlternates(Arrays.asList(new AlternateCoordinate("1", 98, 102, "ATCTG", "", VariantType.INDEL)));

        Variant regA = getVariant("1:99:A:.", studyId, sampleName1, "0/0",  map("FILTER","PASS"));
        Variant regB = getVariant("1:101:A:.", studyId, sampleName1, "0/0",  map("FILTER","PASS"));

        calculator.addVariant(regA);
        calculator.addVariant(variant);
        calculator.addVariant(regB);
        calculator.fillNoCalls(Arrays.asList(sampleName1, sampleName1), 98, 103);

        assertEquals(1, calculator.getReference(98).size());
        assertEquals(1, calculator.getReference(98, -1).size());
        assertEquals(1, calculator.getReference(99).size());
        assertEquals(1, calculator.getReference(99, 2).size());
        assertEquals(1, calculator.getReference(100).size());
        assertEquals(1, calculator.getReference(101).size());
        assertEquals(1, calculator.getReference(101, 2).size());

        assertEquals(1, calculator.getAlt(100, "+").size());

        assertEquals(1, calculator.getPass(99).size());
        assertEquals(1, calculator.getPass(101).size());


        assertEquals(1, calculator.getNotPass(98).size());
        assertEquals(1, calculator.getNotPass(100).size());
        assertEquals(1, calculator.getNotPass(102).size());
    }

    @Test
    public void calculate() throws Exception {
        String studyId = "22";
        Map<String, Integer> idMapping = new HashMap<>();
        String sampleName1 = "Sample 1";
        idMapping.put(sampleName1, 1);
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator(studyId, idMapping);

        Variant variant = new Variant("1:100:-:AT");
        StudyEntry studyEntry = new StudyEntry(null, studyId);
        studyEntry.setSecondaryAlternates(Arrays.asList(new AlternateCoordinate("1", 98, 102, "ATCTG", "", VariantType.INDEL)));

        Map<String, Integer> samplePos = new HashMap<>();
        samplePos.put(sampleName1, 0);
        studyEntry.setSamplesPosition(samplePos);
        studyEntry.setFormat(Arrays.asList("GT", "AD"));
        studyEntry.setSamplesData(Arrays.asList(Arrays.asList("1/2", "1,12,13")));

        FileEntry file = new FileEntry("1","1/2", map("FILTER","not-pass"));
        studyEntry.setFiles(Collections.singletonList(file));

        variant.setStudies(Collections.singletonList(studyEntry));
        calculator.addVariant(variant);
        calculator.fillNoCalls(Arrays.asList(sampleName1, sampleName1), 98, 103);

        assertEquals(1, calculator.getReference(98).size());
        assertEquals(0, calculator.getReference(99).size());
        assertEquals(0, calculator.getReference(100).size());
        assertEquals(1, calculator.getReference(101).size());
        assertEquals(1, calculator.getReference(102).size());

        assertEquals(1, calculator.getAlt(100, "*").size());
        assertEquals(1, calculator.getAlt(100, "+").size());
        assertEquals(1, calculator.getVariant(100, "_AT").size());

        assertEquals(1, calculator.getNotPass(98).size());
        assertEquals(1, calculator.getNotPass(99).size());
        assertEquals(1, calculator.getNotPass(100).size());
        assertEquals(1, calculator.getNotPass(101).size());
        assertEquals(1, calculator.getNotPass(102).size());
    }

    @Test
    public void setRegionRef() throws Exception {
        int sampleId = 10;
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap(), 100, 102);
        Map<Integer, AlleleInfo> allelecnt = map(0, new AlleleInfo(1, 0));
        calculator.updateReferenceCount(sampleId, new RegionImpl<>(allelecnt, 99, 102));

        assertNull(calculator.buildPositionCount(99));
        assertNotNull(calculator.buildPositionCount(100));
        assertNotNull(calculator.buildPositionCount(101));
        assertNull(calculator.buildPositionCount(102));
    }

    @Test
    public void setRegionAlt() throws Exception {
        int sampleId = 10;
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap(), 100, 102);
        String[] refAlt = {"ABC", ""};
        String vid = AlleleInfo.buildVariantId(refAlt);

        RegionImpl<AlleleInfo> region = new RegionImpl<>(new AlleleInfo(1, 0), 99, 102);
        region.getData().setType(VariantType.INDEL);
        region.getData().setId(refAlt);
        calculator.updateAlternateCount(sampleId, region);

        assertNull(calculator.buildVariantCount(99, vid));
        assertNull(calculator.buildVariantCount(100, vid));
        assertNull(calculator.buildVariantCount(101, vid));
        assertNull(calculator.buildVariantCount(102, vid));


        assertNull(calculator.buildPositionCount(99));
        assertNotNull(calculator.buildPositionCount(100));
        assertNotNull(calculator.buildPositionCount(101));
        assertNull(calculator.buildPositionCount(102));

        assertTrue(calculator.getAlt(100, HBaseAlleleCalculator.DEL_SYMBOL, 1).contains(sampleId));
    }

// // NOT used any more
////    @org.junit.Test
////    public void onlyLeaveSparseRepresentation() {
////        String studyId = "22";
////        Map<String, Integer> idMapping = new HashMap<>();
////        String sampleName1 = "Sample 1";
////        String sampleName2 = "Sample 2";
////        idMapping.put(sampleName1, 1);
////        idMapping.put(sampleName2, 2);
////        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator(studyId, idMapping);
////
////        Variant variant = new Variant("1:100:AT:-");
////        StudyEntry studyEntry = new StudyEntry(null, studyId);
////
////        Map<String, Integer> samplePos = new HashMap<>();
////        samplePos.put(sampleName1, 0);
////        samplePos.put(sampleName2, 1);
////        studyEntry.setSamplesPosition(samplePos);
////        studyEntry.setFormat(Arrays.asList("GT"));
////        studyEntry.setSamplesData(Arrays.asList(Arrays.asList("0/1"), Arrays.asList("0/0")));
////
////        FileEntry file = new FileEntry("1","0/1", map("FILTER","PASS"));
////        studyEntry.setFiles(Collections.singletonList(file));
////
////        variant.setStudies(Collections.singletonList(studyEntry));
////        calculator.addVariant(variant);
////        calculator.fillNoCalls(Arrays.asList(sampleName1, sampleName2), 99, 103);
////
////
////
////        System.out.println(calculator.getReferenceToGtToSamples());
////        assertEquals(2, calculator.getReferenceToGtToSamples().get(100).size());
////        System.out.println(calculator.getAlternateToGtToSamples());
////        assertEquals(2, calculator.getAlternateToGtToSamples().get(100).size());
////        System.out.println(calculator.getPassPosition());
////        assertEquals(2, calculator.getPassPosition().size());
////        System.out.println(calculator.getNotPassPosition());
////        assertEquals(2, calculator.getNotPassPosition().size());
////
////        calculator.onlyLeaveSparseRepresentation(99, 103);
////        System.out.println("------------------");
////
////        System.out.println(calculator.getAlternateToGtToSamples());
////        System.out.println(calculator.getReferenceToGtToSamples());
////        System.out.println(calculator.getPassPosition());
////        System.out.println(calculator.getNotPassPosition());
////
////        assertEquals(1, calculator.getReferenceToGtToSamples().get(100).size());
////        assertEquals(2, calculator.getAlternateToGtToSamples().get(100).size());
////        assertEquals(0, calculator.getPassPosition().size());
////        assertEquals(2, calculator.getNotPassPosition().size());
////
////
////        calculator.onlyLeaveSparseRepresentation(100, 103);
////
////        System.out.println(calculator.getAlternateToGtToSamples());
////        System.out.println(calculator.getReferenceToGtToSamples());
////        System.out.println(calculator.getPassPosition());
////        System.out.println(calculator.getNotPassPosition());
////
////        assertEquals(1, calculator.getReferenceToGtToSamples().get(100).size());
////        assertEquals(2, calculator.getAlternateToGtToSamples().get(100).size());
////        assertEquals(0, calculator.getPassPosition().size());
////        assertEquals(1, calculator.getNotPassPosition().size());
////    }
////
////
    @Test
    public void fillNoCalls() {
        String studyId = "22";
        Map<String, Integer> idMapping = new HashMap<>();
        String sampleName = "Sample 1";
        idMapping.put(sampleName, 1);
        AbstractAlleleCalculator calculator = new HBaseAlleleCalculator(studyId, idMapping);

        Variant variant = new Variant("1:100:AT:-");
        StudyEntry studyEntry = new StudyEntry(null, studyId);

        Map<String, Integer> samplePos = new HashMap<>();
        samplePos.put(sampleName, 0);
        studyEntry.setSamplesPosition(samplePos);
        studyEntry.setFormat(Arrays.asList("GT"));
        studyEntry.setSamplesData(Arrays.asList(Arrays.asList("0/1")));

        FileEntry file = new FileEntry("1","0/1", map("FILTER","PASS"));
        studyEntry.setFiles(Collections.singletonList(file));

        variant.setStudies(Collections.singletonList(studyEntry));
        calculator.addVariant(variant);

        assertEquals(0, calculator.getPass(99).size());
        assertEquals(0, calculator.getNotPass(99).size());
        assertEquals(1, calculator.getPass(100).size());
        assertEquals(1, calculator.getPass(101).size());
        assertEquals(0, calculator.getPass(102).size());
        assertEquals(0, calculator.getNotPass(102).size());

        calculator.fillNoCalls(Collections.singleton(sampleName), 99, 103);

        assertEquals(1, calculator.getNotPass(99).size());
        assertEquals(0, calculator.getNotPass(100).size());
        assertEquals(0, calculator.getNotPass(101).size());
        assertEquals(1, calculator.getNotPass(102).size());

    }

    @Test
    public void updateReferenceCount_NOCALL() {
        int sampleId = 10;
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap());
        Map<Integer, AlleleInfo> alleleCount = new HashMap<>();
        alleleCount.put(-1, new AlleleInfo(1, 0));

        assertEquals(1, alleleCount.size());
        calculator.updateReferenceCount(sampleId, new RegionImpl<>(alleleCount, 100, 102));

        assertEquals(0, alleleCount.size()); // make sure it is removed
        for (int i = 100; i < 103; ++i) {
            AlleleCountPosition position = calculator.buildPositionCount(i);
            Map<Integer, List<Integer>> ref = position.getReference();
            assertEquals(1, ref.size());
            assertNotNull(ref.get(-1));
            assertTrue(ref.get(-1).contains(sampleId));
        }
    }


    @Test
    public void  updateAlternateCount_SNP() {
        int sampleId = 10;
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap());

        String alt = "T";
        String[] refAlt = {"V", alt};
        String vid = AlleleInfo.buildVariantId(refAlt);
        int start = 100;
        int end = 100;
        RegionImpl<AlleleInfo> region = new RegionImpl<>(new AlleleInfo(1, 0), start, end);
        region.getData().setType(VariantType.SNP);
        region.getData().setId(refAlt);
        calculator.updateAlternateCount(sampleId, region);

        assertNotNull(calculator.buildVariantCount(100, vid));
        assertNotNull(calculator.buildVariantCount(100, vid).getAlternate().get(1));
        assertTrue(calculator.buildVariantCount(100, vid).getAlternate().get(1).contains(sampleId));

        assertNotNull(calculator.buildPositionCount(100));
        assertNotNull(calculator.getAlt(100).get(alt));
        assertNotNull(calculator.getAlt(100, alt).get(1));
        assertTrue(calculator.getAlt(100, alt, 1).contains(sampleId));

    }

    @Test
    public void  updateAlternateCount_DELETION() {
        int sampleId = 10;
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap());

        Variant var = new Variant("1:100:ATG:-");
        String[] refAlt = {"ATG", ""};
        String vid = AlleleInfo.buildVariantId(refAlt);
        int start = var.getStart();
        int end = var.getEnd();
        RegionImpl<AlleleInfo> region = new RegionImpl<>(new AlleleInfo(1, 0), start, end);
        region.getData().setType(VariantType.INDEL);
        region.getData().setId(refAlt);
        calculator.updateAlternateCount(sampleId, region);

        assertNotNull(calculator.buildVariantCount(start, vid));
        assertNotNull(calculator.buildVariantCount(start, vid).getAlternate().get(1));
        assertTrue(calculator.buildVariantCount(start, vid).getAlternate().get(1).contains(sampleId));

        // Check  *
        assertNotNull(calculator.buildPositionCount(start));
        assertNotNull(calculator.buildPositionCount(start).getAltMap().get("*"));
        assertNotNull(calculator.buildPositionCount(start).getAltMap().get("*").get(1));
        assertTrue(calculator.buildPositionCount(start).getAltMap().get("*").get(1).contains(sampleId));
        assertTrue(calculator.buildPositionCount(start + 1).getAltMap().get("*").get(1).contains(sampleId));
        assertTrue(calculator.buildPositionCount(start + 2).getAltMap().get("*").get(1).contains(sampleId));
        assertNull(calculator.buildPositionCount(start + 2).getAltMap().get("*").get(2));

        refAlt = new String[] {"TG", ""};
        vid = AlleleInfo.buildVariantId(refAlt);
        start += 1;
        region = new RegionImpl<>(new AlleleInfo(1, 0), start, end);
        region.getData().setType(VariantType.INDEL);
        region.getData().setId(refAlt);
        calculator.updateAlternateCount(sampleId, region);

        assertNotNull(calculator.buildVariantCount(start, vid));
        assertNotNull(calculator.buildVariantCount(start, vid).getAlternate().get(1));
        assertTrue(calculator.buildVariantCount(start, vid).getAlternate().get(1).contains(sampleId));

        assertTrue(calculator.buildPositionCount(start-1).getAltMap().get("*").get(1).contains(sampleId));
        assertNotNull(calculator.buildPositionCount(start).getAltMap().get("*"));
        assertNull(calculator.buildPositionCount(start).getAltMap().get("*").get(1));
        assertNull(calculator.buildPositionCount(start + 1).getAltMap().get("*").get(1));
        assertTrue(calculator.buildPositionCount(start).getAltMap().get("*").get(2).contains(sampleId));
        assertTrue(calculator.buildPositionCount(start + 1).getAltMap().get("*").get(2).contains(sampleId));

    }

    @Test
    public void updateReferenceCount_REF_CALL() {
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap());
        int sampleId = 10;
        Map<Integer, AlleleInfo> alleleCount = new HashMap<>();
        alleleCount.put(0, new AlleleInfo(1, 0));
        alleleCount.put(1, new AlleleInfo(1, 0));

        assertEquals(2, alleleCount.size());
        calculator.updateReferenceCount(sampleId, new RegionImpl<>(alleleCount, 100, 102));
        assertEquals(1, alleleCount.size()); // make sure it is removed

        for (int i = 100; i < 103; i++) {
            AlleleCountPosition position = calculator.buildPositionCount(i);
            Map<Integer, List<Integer>> ref = position.getReference();
            assertEquals(1, ref.size());
            assertNotNull("Problems with position " + i, ref.get(1));
            assertNull(ref.get(2));
            assertTrue(ref.get(1).contains(sampleId));
        }
    }

    @Test
    public void updateReferenceCount_HOM_REF_CALL() {
        HBaseAlleleCalculator calculator = new HBaseAlleleCalculator("a", Collections.emptyMap());
        int sampleId = 10;
        Map<Integer, AlleleInfo> alleleCount = new HashMap<>();
        alleleCount.put(0, new AlleleInfo(2, 0));
        alleleCount.put(1, new AlleleInfo(1, 0));

        assertEquals(2, alleleCount.size());
        calculator.updateReferenceCount(sampleId, new RegionImpl<>(alleleCount, 100, 102));
        assertEquals(1, alleleCount.size()); // make sure it is removed

        for (int i = 100; i < 103; i++) {
            Map<Integer, List<Integer>> ref = calculator.buildPositionCount(i).getReference();
            assertEquals(1, ref.size());
            assertNotNull(ref.get(2));
            assertNull(ref.get(1));
            assertTrue(ref.get(2).contains(sampleId));
        }
    }

    @Test
    public void buildVariantId() {
        Variant var = new Variant("1:100:A:C");
        List<AlternateCoordinate> alts = Arrays.asList(
                new AlternateCoordinate("1",100,102, "ATC","", VariantType.INDEL),
                new AlternateCoordinate("1",100,99, "","ABC", VariantType.INDEL)
        );
        assertEquals("", AlleleInfo.buildVariantId("V_", AbstractAlleleCalculator.buildRefAlt(var, null, 0)));
        assertEquals("V_A_C", AlleleInfo.buildVariantId("V_", AbstractAlleleCalculator.buildRefAlt(var, null, 1)));
        assertEquals("V_ATC_", AlleleInfo.buildVariantId("V_", AbstractAlleleCalculator.buildRefAlt(var, alts, 2)));
        assertEquals("V__ABC", AlleleInfo.buildVariantId("V_", AbstractAlleleCalculator.buildRefAlt(var, alts, 3)));
    }

    @Test
    public void getAlleleCount() {
        assertEquals(map(-1, 1), AbstractAlleleCalculator.getAlleleCount("."));
        assertEquals(map(-1, 1), AbstractAlleleCalculator.getAlleleCount("./."));
        assertEquals(map(0, 2), AbstractAlleleCalculator.getAlleleCount("0/0"));
        assertEquals(map(0, 1), AbstractAlleleCalculator.getAlleleCount("0"));
        assertEquals(map(0, 1), AbstractAlleleCalculator.getAlleleCount("./0"));
        assertEquals(map(0, 1, 1, 1), AbstractAlleleCalculator.getAlleleCount("0/1"));
        assertEquals(map(1, 1), AbstractAlleleCalculator.getAlleleCount("./1"));
    }

    public static <K,V> Map<K, V> mapObj(K k, V v, Object ... ints) {
        Map<K, V> map = new HashMap<>();
        map.put(k, v);

        for (int i = 0; i < ints.length; i += 2) {
            map.put((K) ints[i], (V) ints[i+1]);
        }
        return map;
    }

    public static <K, V> Map<K, V>  map(K k, V v) {
        Map<K, V> map = new HashMap<>();
        map.put(k, v);
        return map;
    }

    public static <T> Map<T, T> map(T ... ints) {
        return mapObj(ints[0], ints[1], Arrays.copyOfRange(ints, 2, ints.length));
    }
}