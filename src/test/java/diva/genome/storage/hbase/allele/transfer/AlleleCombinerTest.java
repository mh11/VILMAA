package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
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

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculatorTest.getVariant;
import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculatorTest.*;
import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.buildVariantId;
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
    private Variant snv;
    private HBaseAlleleCalculator calculator;
    private Variant insertion;
    private Variant deletion;
    private Variant deletionAndInsertion;
    private Variant insertionAndDeletion;
    private Variant insertionAndSNP;
    private StudyConfiguration singleStudyConfiguration;
    private VariantTableHelper variantTableHelper;
    private HBaseAlleleCountsToVariantConverter variantConverter;

    @Before
    public void setup() {
        this.snv = getVariant(chromosome + ":" + position + ":A:G", studyId, sampleName, "0/1", map("FILTER", "not-pass"));
        this.calculator = new HBaseAlleleCalculator("22", (Map<String, Integer>) mapObj(sampleName, sampleId));
        this.insertion = getVariant(chromosome + ":" + position + ":-:G", studyId, sampleName, "0/1", map("FILTER", "not-pass"));
        this.deletion = getVariant(chromosome + ":" + position + ":GT:-", studyId, sampleName, "0/1", map("FILTER", "PASS"));

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

        this.singleStudyConfiguration.setSamplesInFiles((Map<Integer, LinkedHashSet<Integer>>) mapObj(fileId, new LinkedHashSet<>(Collections.singleton(sampleId))));
        

        Configuration conf = new Configuration();
        VariantTableHelper.setInputTableName(conf, "input");
        VariantTableHelper.setOutputTableName(conf, "output");
        VariantTableHelper.setStudyId(conf, studyIdInt);
        this.variantTableHelper = new VariantTableHelper(conf);

        variantConverter = new HBaseAlleleCountsToVariantConverter(this.variantTableHelper, this.singleStudyConfiguration);
        this.variantConverter.setReturnSamples(Collections.singleton(this.sampleName));
    }

    @Test
    public void combineSNV() throws Exception {
        calculator.addVariant(snv);
        AlleleCountPosition validate = validate(new HashSet<Integer>(Arrays.asList(sampleId)), snv,
                Collections.emptyMap(),
                (Map<Integer, Set<Integer>>) mapObj(1, new HashSet<>(Arrays.asList(sampleId))));
        Variant variant = convertBack(this.snv, validate);
        System.out.println("snv = " + snv.getImpl());
        equals(snv, variant);
    }

    public void equals(Variant expected, Variant actual) {
        equals(expected, expected.getStudy(studyId).getSampleData(sampleName ,"GT"), actual);
    }

    public void equals(Variant expected, String expectedGt, Variant actual) {
        equalsGT(expectedGt, actual);
        assertEquals(extractFilterValue(expected), actual.getStudy(studyId).getSampleData(sampleName ,"FT"));
    }

    public void equalsGT(String gt, Variant actual) {
        assertEquals(gt, actual.getStudy(studyId).getSampleData(sampleName ,"GT"));
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
        variant = this.variantConverter.fillVariant(variant, validate);
        System.out.println("variant = " + variant.getImpl());
        return variant;
    }

    public Variant convertBack(AlternateCoordinate input, AlleleCountPosition validate) {
        Variant variant = new Variant(input.getChromosome(), input.getStart(), input.getEnd(), input.getReference(), input.getAlternate(), "+");
        variant.setType(input.getType());
        variant = this.variantConverter.fillVariant(variant, validate);
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
                (Map<Integer, Set<Integer>>) mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

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
                (Map<Integer, Set<Integer>>) mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

        Variant variant = convertBack(this.deletionAndInsertion, validate);
        System.out.println("deletionAndInsertion = " + deletionAndInsertion.getImpl());
        equals(deletionAndInsertion, "0/1", variant);
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
                (Map<Integer, Set<Integer>>) mapObj(1, new HashSet<>(Arrays.asList(sampleId))));

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
                (Map<Integer, Set<Integer>>) mapObj(1, new HashSet<>(Arrays.asList(sampleId))));
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
    public void combineInsertionAndSNP() throws Exception {
        calculator.addVariant(insertionAndSNP);
        HashMap<Integer, Map<Integer, Integer>> overlaps = new HashMap<>();
        overlaps.put(insertionAndSNP.getEnd(), map(sampleId, 1));
        overlaps.put(position -1, map(sampleId, 1));
        AlleleCountPosition validate = validate(new HashSet<>(Arrays.asList(sampleId)), insertionAndSNP, overlaps,
                (Map<Integer, Set<Integer>>) mapObj(1, Collections.singleton(sampleId)));
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

    protected AlleleCountPosition validate(Set<Integer> sampleIds, Variant variant,
                                           Map<Integer, Map<Integer, Integer>> overlaps, Map<Integer, Set<Integer>>  expectedReference) {
        return validate(sampleIds, variant, buildVariantId(StringUtils.EMPTY, variant.getReference(), variant.getAlternate()),
                overlaps, expectedReference);
    }

    protected AlleleCountPosition validate(Set<Integer> sampleIds, Variant variant, String varId,
                                           Map<Integer, Map<Integer, Integer>> overlaps, Map<Integer, Set<Integer>>  expectedReference) {
        Integer position = variant.getStart();
        String chromosome = variant.getChromosome();

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

    protected Map<Integer, List<Integer>> convertSetToList(Map<Integer, Set<Integer>> integerSetMap) {
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

    protected Map<Integer, Set<Integer>> convertListToSet(Map<Integer, List<Integer>> integerSetMap) {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        integerSetMap.forEach((k, v) -> map.put(k, new HashSet<>(v)));
        return map;
    }

}