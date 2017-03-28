package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleInfo;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import diva.genome.storage.hbase.allele.count.position.AbstractAlleleCalculator;
import diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator;
import diva.genome.storage.hbase.allele.count.region.AlleleRegionCalculator;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Before;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.util.*;

import static diva.genome.storage.hbase.allele.count.AlleleInfo.buildVariantId;
import static diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculatorTest.*;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Created by mh719 on 03/02/2017.
 */
public class AlleleCombinerTest {
    private Integer position = 123;
    private String chromosome = "10";
    private Integer fileId = 1;
    private String fileIdString = fileId.toString();
    private Integer sampleId = 1;
    private String sampleName = "S1";
    private Integer studyIdInt = 22;
    private String studyId = studyIdInt.toString();
    private Variant homrefOnAuto;
    private Variant hemiref;
    private Variant snv;
    private Variant snv2;
    private AbstractAlleleCalculator calculator;
    private Variant reference2;
    private Variant insertion;
    private Variant insertion2;
    private Variant deletion;
    private Variant deletionS2;
    private Variant deletionAndInsertion;
    private Variant insertionAndDeletion;
    private Variant insertionAndSNP;
    private Variant snpAndInsertion;
    private StudyConfiguration singleStudyConfiguration;
    private VariantTableHelper variantTableHelper;
    private HBaseAlleleCountsToVariantConverter variantConverter;

    @Before
    public void setup() {
        this.reference2 = getVariant(chromosome + ":" + position + ":.:.", studyId, 2, "S2", "0/0", map("FILTER", "not-pass"));
        this.snv = getVariant(chromosome + ":" + position + ":A:G", studyId, sampleName, "0/1", map("FILTER", "not-pass"));
        this.snv2 = getVariant(chromosome + ":" + position + ":A:G", studyId, 2, "S2", "0/1", map("FILTER", "not-pass"));
//        this.calculator = new HBaseAlleleCalculator("22", mapObj(sampleName, sampleId));
        this.calculator = new AlleleRegionCalculator("22", mapObj(sampleName, sampleId), position - 2, position + 2);
        this.insertion = getVariant(chromosome + ":" + position + ":-:G", studyId, sampleName, "0/1", map("FILTER", "not-pass"));
        this.insertion2 = getVariant(chromosome + ":" + position + ":-:GTT", studyId, 2, "S2", "0/1", map("FILTER", "not-pass"));
        this.deletion = getVariant(chromosome + ":" + position + ":GT:-", studyId, sampleName, "0/1", map("FILTER", "PASS"));
        this.deletionS2 = getVariant(chromosome + ":" + position + ":GTTT:-", studyId, 2, "S2", "0/1", map("FILTER", "PASS"));

        this.deletionAndInsertion = getVariant(chromosome + ":" + position + ":GT:-", studyId, sampleName, "1/2", map("FILTER", "not-pass"));
        this.deletionAndInsertion.getStudy(studyId).setSecondaryAlternates(Arrays.asList(new AlternateCoordinate(chromosome, position, position-1, "", "AT", VariantType.INDEL)));

        this.insertionAndDeletion = getVariant(chromosome + ":" + position + ":-:AT", studyId, sampleName, "1/2", map("FILTER", "not-pass"));
        this.insertionAndDeletion.getStudy(studyId).setSecondaryAlternates(Arrays.asList(new AlternateCoordinate(chromosome, position, position+1, "GT", "", VariantType.INDEL)));

        this.insertionAndSNP = getVariant(chromosome + ":" + position + ":-:AT", studyId, sampleName, "1/2", map("FILTER", "not-pass"));
        this.insertionAndSNP.getStudy(studyId).setSecondaryAlternates(Arrays.asList(new AlternateCoordinate(chromosome, position, position, "A", "T", VariantType.SNP)));

        this.singleStudyConfiguration = new StudyConfiguration(studyIdInt, studyId);
        this.singleStudyConfiguration.setIndexedFiles(new LinkedHashSet<>(Collections.singleton(1)));
        this.singleStudyConfiguration.setSampleIds(map(sampleName, 1));
        this.singleStudyConfiguration.setFileIds(map("file1", 1));
        this.singleStudyConfiguration.setSamplesInFiles(mapObj(fileId, new LinkedHashSet<>(Collections.singleton(sampleId))));



        this.homrefOnAuto = getVariant(chromosome + ":" + position + ":.:." , studyId, 2,"S2", "0/0", map("FILTER", "not-pass"));
        this.hemiref = getVariant(chromosome + ":" + position + ":.:." , studyId, 2,"S2", "0", map("FILTER", "not-pass"));

        Configuration conf = new Configuration();
        VariantTableHelper.setInputTableName(conf, "input");
        VariantTableHelper.setOutputTableName(conf, "output");
        VariantTableHelper.setStudyId(conf, studyIdInt);
        this.variantTableHelper = new VariantTableHelper(conf);

        variantConverter = new HBaseAlleleCountsToVariantConverter(this.variantTableHelper, this.singleStudyConfiguration);
        this.variantConverter.setReturnSamples(Collections.singleton(this.sampleName));
    }

    public void setupTwoSamples() {
        this.calculator = new HBaseAlleleCalculator("22", mapObj(sampleName, sampleId, "S2", 2));
        this.singleStudyConfiguration = new StudyConfiguration(studyIdInt, studyId);
        this.singleStudyConfiguration.setFileIds(mapObj("file1", 1, "file2", 2));
        this.singleStudyConfiguration.setIndexedFiles(new LinkedHashSet<>(Arrays.asList(sampleId, 2)));
        this.singleStudyConfiguration.setSampleIds(mapObj(sampleName, 1, "S2", 2));
        this.singleStudyConfiguration.setSamplesInFiles(mapObj(fileId, new LinkedHashSet<>(Arrays.asList(sampleId)), 2, new LinkedHashSet<>(Arrays.asList( 2))));

        variantConverter = new HBaseAlleleCountsToVariantConverter(this.variantTableHelper, this.singleStudyConfiguration);
        this.variantConverter.setReturnSamples(Arrays.asList(this.sampleName, "S2"));

    }

    @Test
    public void combineDeletionAndDeletionSeparate() {
        setupTwoSamples();
        calculator.addVariant(deletion);
        calculator.addVariant(deletionS2);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletion.getEnd(), map(sampleId, 1));
        overlaps.put(deletionS2.getEnd(), map(2, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), deletion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(sampleId, 2))));

        Variant variant = convertBack(this.deletion, validate);
        System.out.println("deletion = " + deletion.getImpl());
        equals(deletion, "0/1", variant);
        equalsGT("S2", "0/2", variant);
    }

    @Test
    public void combineDeletionAndDeletionSeparateB() {
        setupTwoSamples();
        calculator.addVariant(deletion);
        calculator.addVariant(deletionS2);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletionS2.getEnd(), map(2, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), deletionS2, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(2))));

        Variant variant = convertBack(this.deletionS2, validate);
        System.out.println("deletionS2 = " + deletionS2.getImpl());
        equalsGT("S2", "0/1", variant);
        equalsGT(sampleName, "0/0", variant);
    }


    @Test
    public void combineInsertionAndSnpSeparate() {
        setupTwoSamples();
        calculator.addVariant(insertion);
        calculator.addVariant(snv2);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(1, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), insertion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(1))));

        Variant variant = convertBack(this.insertion, validate);
        System.out.println("insertion = " + insertion.getImpl());
        equalsGT("S2", "0/0", variant);
        equalsGT(sampleName, "0/1", variant);
    }

    @Test
    public void combineInsertionAndInsertionSeparate() {
        setupTwoSamples();
        calculator.addVariant(insertion);
        calculator.addVariant(insertion2);
        calculator.addVariant(reference2);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion2.getEnd(), map(1, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), insertion2, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(1,2))));

        Variant variant = convertBack(this.insertion2, validate);
        System.out.println("insertion2 = " + insertion2.getImpl());
        equalsGT("S2", "0/1", variant);
        equalsGT(sampleName, "0/2", variant);
    }

    @Test
    public void combineInsertionAndInsertionSnvSeparate() {
        setupTwoSamples();
        calculator.addVariant(insertion);
        calculator.addVariant(insertion2);
        calculator.addVariant(snv2);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(1, 1, 2, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), insertion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(1,2))));

        Variant variant = convertBack(this.insertion, validate);
        System.out.println("insertion = " + insertion.getImpl());
        equalsGT(sampleName, "0/1", variant);
        equalsGT("S2", "0/2", variant);
    }

    @Test
    public void combineSnvAndInsertionSeparateSameSample() {
        calculator.addVariant(snv);
        calculator.addVariant(insertion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), snv, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

        Variant variant = convertBack(this.snv, validate);
        System.out.println("snv = " + snv.getImpl());
        equalsGT(sampleName, "0/1", variant);
    }


    @Test
    public void combineRefCall() throws Exception {
        setupTwoSamples();
        calculator.addVariant(homrefOnAuto);
        calculator.addVariant(snv);
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), snv,
                Collections.emptyMap(), mapObj(1, new HashSet<>(Arrays.asList(sampleId))));
        Variant variant = convertBack(this.snv, validate);
        System.out.println("ref = " + homrefOnAuto.getImpl());
        equalsGT("S2", "0/0", variant);
    }

    @Test
    public void combineHemiRefCall() throws Exception {
        setupTwoSamples();
        calculator.addVariant(hemiref);
        calculator.addVariant(snv);
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), snv,
                Collections.emptyMap(), mapObj(1, new HashSet<>(Arrays.asList(sampleId, 2))));
        Variant variant = convertBack(this.snv, validate);
        System.out.println("ref = " + hemiref.getImpl());
        equalsGT("S2", "0", variant);
    }

    @Test
    public void combineSNV() throws Exception {
        calculator.addVariant(snv);
        AlleleCountPosition validate = validate(new HashSet<Integer>(Arrays.asList(sampleId)), snv,
                Collections.emptyMap(), mapObj(1, new HashSet<>(Arrays.asList(sampleId))));
        Variant variant = convertBack(this.snv, validate);
        System.out.println("snv = " + snv.getImpl());
        equals(snv, variant);
    }

    public void equals(Variant expected, Variant actual) {
        equals(expected, expected.getStudy(studyId).getSampleData(sampleName ,"GT"), actual);
    }

    public void equals(Variant expected, String expectedGt, Variant actual) {
        equals(expected, sampleName, expectedGt, actual);
    }

    public void equals(Variant expected, String sample, String expectedGt, Variant actual) {
        equalsGT(sample, expectedGt, actual);
        assertEquals(extractFilterValue(expected), actual.getStudy(studyId).getSampleData(sample ,"FT"));
    }

    public void equalsGT(String gt, Variant actual) {
        equalsGT(sampleName, gt, actual);
    }

    public void equalsGT(String sample, String gt, Variant actual) {
        assertEquals(gt, actual.getStudy(studyId).getSampleData(sample ,"GT"));
    }

    public String extractFilterValue(Variant expected) {
        String filter = expected.getStudy(studyId).getFile(this.fileIdString).getAttributes().getOrDefault("FILTER",
                VariantMerger.DEFAULT_FILTER_VALUE);
        if (!filter.equals(VariantMerger.PASS_VALUE)) {
            return VariantMerger.DEFAULT_FILTER_VALUE;
        }
        return filter;
    }

    public Variant convertBack(Variant input, AlleleCountPosition validate) {
        Variant variant = new Variant(input.getChromosome(), input.getStart(), input.getEnd(), input.getReference(), input.getAlternate(), input.getStrand());
        variant.setType(input.getType());
        variant = this.variantConverter.doConvert(variant, validate);
        System.out.println("variant = " + variant.getImpl());
        return variant;
    }

    public Variant convertBack(AlternateCoordinate input, AlleleCountPosition validate) {
        Variant variant = new Variant(input.getChromosome(), input.getStart(), input.getEnd(), input.getReference(), input.getAlternate(), "+");
        variant.setType(input.getType());
        variant = this.variantConverter.doConvert(variant, validate);
        System.out.println("variant = " + variant.getImpl());
        return variant;
    }

    @Test
    public void combineInsertion() throws Exception {
        calculator.addVariant(insertion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertion, overlaps,
                (Map<Integer, Set<Integer>>) mapObj(1, Collections.singleton(sampleId)));
        Variant variant = convertBack(this.insertion, validate);
        System.out.println("insertion = " + insertion.getImpl());
        equals(insertion, variant);
    }

    @Test
    public void combineDeletion() throws Exception {
        calculator.addVariant(deletion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), deletion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

        Variant variant = convertBack(this.deletion, validate);
        System.out.println("deletion = " + deletion.getImpl());
        equals(deletion, variant);
    }

    @Test
    public void combineDeletionAndInsertion() throws Exception {
        calculator.addVariant(deletionAndInsertion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletionAndInsertion.getEnd(), map(sampleId, 1));
        overlaps.put(position -1, map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), deletionAndInsertion, overlaps,
//                Collections.emptyMap());
                mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

        Variant variant = convertBack(this.deletionAndInsertion, validate);
        System.out.println("deletionAndInsertion = " + deletionAndInsertion.getImpl());
        equals(deletionAndInsertion, "0/1", variant);
    }

    @Test
    public void combineDeletionAndDeletionInsertion() throws Exception {
        setupTwoSamples();
        calculator.addVariant(deletionS2);
        calculator.addVariant(deletionAndInsertion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletionAndInsertion.getEnd(), map(sampleId, 1));
        overlaps.put(deletionAndInsertion.getStudy(studyId).getSecondaryAlternates().get(0).getEnd(), map(sampleId, 1));
        overlaps.put(deletionS2.getEnd(), map(2, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), deletionS2, overlaps,
//                Collections.emptyMap());
                mapObj(1, new HashSet<>(Arrays.asList(2))));

        Variant variant = convertBack(this.deletionS2, validate);
        System.out.println("deletionS2 = " + deletionS2.getImpl());
        equalsGT("S2", "0/1", variant);
        equalsGT(sampleName, "0/0", variant);
    }

    @Test
    public void combineDeletionAndInsertionDeletion() throws Exception {
        setupTwoSamples();

        this.insertionAndDeletion = getVariant(chromosome + ":" + (position + 2) + ":-:T", studyId, sampleName, "1/2", map("FILTER", "not-pass"));
        this.insertionAndDeletion.getStudy(studyId).setSecondaryAlternates(Arrays.asList(
                new AlternateCoordinate(chromosome, deletionS2.getStart(), deletionS2.getEnd()-1, deletionS2.getReference(), "GTT", VariantType.INDEL)));

        calculator.addVariant(deletionS2);
        calculator.addVariant(insertionAndDeletion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertionAndDeletion.getEnd(), map(sampleId, 1));
        overlaps.computeIfAbsent(insertionAndDeletion.getStudy(studyId).getSecondaryAlternates().get(0).getEnd(), k -> new HashMap<>()).put(sampleId, 1);
        overlaps.computeIfAbsent(deletionS2.getEnd(), k -> new HashMap<>()).put(2, 1);
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId, 2)), deletionS2, overlaps,
//                Collections.emptyMap());
                mapObj(1, new HashSet<>(Arrays.asList(2))));

        Variant variant = convertBack(this.deletionS2, validate);
        System.out.println("deletionS2 = " + deletionS2.getImpl());
        equalsGT("S2", "0/1", variant);
        equalsGT(sampleName, "0/0", variant);
    }

    @Test
    public void combineDeletionAndInsertionSeparateA() throws Exception {
        calculator.addVariant(deletion);
        calculator.addVariant(insertion);
        combineDeletionAndInsertionSeparate();
    }

    @Test
    public void combineDeletionAndInsertionSeparateB() throws Exception {
        calculator.addVariant(insertion);
        calculator.addVariant(deletion);
        combineDeletionAndInsertionSeparate();
    }


    protected void combineDeletionAndInsertionSeparate() {
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(deletion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), deletion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

        Variant variant = convertBack(this.deletion, validate);
        deletion.getStudy(studyId).getFile(fileIdString).getAttributes().put("FILTER", ".");
        System.out.println("deletion = " + deletion.getImpl());
        equals(deletion, "0/1", variant);
    }

    @Test
    public void combineInsertionAndDeletionSeparateA() throws Exception {
        calculator.addVariant(deletion);
        calculator.addVariant(insertion);
        combineInsertionAndDeletionSeparate();
    }

    @Test
    public void combineInsertionAndDeletionSeparateB() throws Exception {
        calculator.addVariant(insertion);
        calculator.addVariant(deletion);
        combineInsertionAndDeletionSeparate();
    }

    protected void combineInsertionAndDeletionSeparate() {
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertion, overlaps,
                mapObj(1, new HashSet<>(Arrays.asList(sampleId))));
        Variant variant = convertBack(this.insertion, validate);
        System.out.println("insertion = " + insertion.getImpl());
        equals(insertion, "0/1", variant);
    }

    @Test
    public void combineInsertionAndDeletion() throws Exception {
        calculator.addVariant(insertionAndDeletion);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertionAndDeletion.getEnd(), map(sampleId, 1));
        overlaps.put(position -1, map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertionAndDeletion,
                overlaps, Collections.emptyMap());
        Variant variant = convertBack(this.insertionAndDeletion, validate);
        AlternateCoordinate secAlt = variant.getStudy(this.studyId).getSecondaryAlternates().get(0);
        System.out.println("insertionAndDeletion = " + insertionAndDeletion.getImpl());
        equals(insertionAndDeletion, variant);
    }

    @Test
    public void combineInsertionAndSNPasSecAlt() throws Exception {
        calculator.addVariant(insertionAndSNP);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertionAndSNP.getEnd(), map(sampleId, 1));
        overlaps.put(position -1, map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertionAndSNP, overlaps,
                mapObj(1, Collections.singleton(sampleId)));
        Variant variant = convertBack(this.insertionAndSNP, validate);
        assertTrue(variant.getStudy(this.studyId).getSecondaryAlternates().isEmpty());
        System.out.println("insertionAndSNP = " + insertionAndSNP.getImpl());
        equals(insertionAndSNP, "0/1", variant);
        AlternateCoordinate secAlt = insertionAndSNP.getStudy(studyId).getSecondaryAlternates().get(0);

        // Test SecAlt as well
        validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertionAndSNP,
                buildVariantId(StringUtils.EMPTY, secAlt.getReference(), secAlt.getAlternate()), overlaps,
                (Map<Integer, Set<Integer>>) mapObj(1, Collections.singleton(sampleId)));

        variant = convertBack(secAlt, validate);
        assertTrue(variant.getStudy(this.studyId).getSecondaryAlternates().isEmpty());
        equals(insertionAndSNP, "0/1", variant);

    }
    @Test
    public void combineInsertionAndSNPSeparate() throws Exception {
        calculator.addVariant(insertion);
        calculator.addVariant(snv);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertion.getEnd(), map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertion, overlaps,
                mapObj(1, Collections.singleton(sampleId)));
        Variant variant = convertBack(this.insertion, validate);
        assertTrue(variant.getStudy(this.studyId).getSecondaryAlternates().isEmpty());
        System.out.println("insertion = " + insertion.getImpl());
        equalsGT(sampleName, "0/1", variant);
    }

    protected AlleleCountPosition validate(Set<Integer> sampleIds, Variant variant,
                                           Map<Integer, Map<Integer, Integer>> overlaps, Map<Integer, Set<Integer>>  expectedReference) {
        return validate(sampleIds, variant, buildVariantId(StringUtils.EMPTY, variant.getReference(), variant.getAlternate()),
                overlaps, expectedReference);
    }

    protected AlleleCountPosition validate(Set<Integer> sampleIds, Variant variant, String varId,
                                           Map<Integer, Map<Integer, Integer>> overlaps, Map<Integer, Set<Integer>>  expectedReference) {
        Integer position = variant.getStart();
        String chromosome = variant.getChromosome();

        calculator.fillNoCalls(this.singleStudyConfiguration.getSampleIds().keySet(), this.position-5, this.position+ 5); // just in case
        calculator.onlyLeaveSparseRepresentation(this.position-5, this.position+ 5, true, true);

        AlleleCountPosition varCount = calculator.buildVariantCount(position, varId);
        AlleleCountPosition refCount = calculator.buildPositionCount(position);


        AlleleCombiner combiner = new AlleleCombiner(sampleIds);
        combiner.combine(variant, refCount, varCount, overlaps);

        Map<Integer, Set<Integer>> actual = ObjectUtils.firstNonNull(convertListToSet(varCount.getReference()), Collections.emptyMap());

        System.out.println("expected = " + expectedReference);
        System.out.println("actual = " + actual);
        assertEquals(expectedReference, actual);
        return varCount;
    }

    public static Map<Integer, List<Integer>> convertSetToList(Map<Integer, Set<Integer>> integerSetMap) {
        Map<Integer, List<Integer>> map = new HashMap<>();
        if (null == integerSetMap || integerSetMap.isEmpty()) {
            return map;
        }
        integerSetMap.forEach((k, v) -> {
            ArrayList<Integer> lst = new ArrayList<>(v);
            Collections.sort(lst);
            map.put(k, lst);
        });
        return map;
    }

    public static Map<Integer, Set<Integer>> convertListToSet(Map<Integer, List<Integer>> integerSetMap) {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        integerSetMap.forEach((k, v) -> map.put(k, new HashSet<>(v)));
        return map;
    }

}