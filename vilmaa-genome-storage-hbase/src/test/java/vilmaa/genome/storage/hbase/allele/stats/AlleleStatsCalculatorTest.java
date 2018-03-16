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

package vilmaa.genome.storage.hbase.allele.stats;

import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator;
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToVariantConverter;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleCombiner;
import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import vilmaa.genome.storage.hbase.allele.count.position.HBaseAlleleCalculatorTest;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleCombinerTest;

import java.util.*;

import static vilmaa.genome.storage.hbase.allele.count.AlleleInfo.buildVariantId;
import static org.junit.Assert.assertEquals;

/**
 * Created by mh719 on 13/02/2017.
 */
public class AlleleStatsCalculatorTest {
    private StudyConfiguration singleStudyConfiguration;
    private Variant snv;
    private Variant ref;
    private VariantTableHelper variantTableHelper;
    private HBaseAlleleCalculator calculator;
    private int position = 123;
    private HBaseAlleleCountsToVariantConverter variantConverter;

    @Test
    public void calculateStats() throws Exception {

        this.ref = HBaseAlleleCalculatorTest.getVariant("Y:123:A:G", "2", 2, "S2", "0", HBaseAlleleCalculatorTest.map("FILTER", "not-pass"));
        this.snv = HBaseAlleleCalculatorTest.getVariant("Y:123:A:G", "2",  1, "S1", "1", HBaseAlleleCalculatorTest.map("FILTER", "not-pass"));

        this.singleStudyConfiguration = new StudyConfiguration(2, "2");
        this.singleStudyConfiguration.setIndexedFiles(new LinkedHashSet<>(Arrays.asList(1, 2)));
        this.singleStudyConfiguration.setSampleIds(HBaseAlleleCalculatorTest.mapObj("S2", 2, "S1", 1));
        this.singleStudyConfiguration.setFileIds(HBaseAlleleCalculatorTest.mapObj("file1", 1, "file2", 2));
        this.singleStudyConfiguration.setSamplesInFiles(HBaseAlleleCalculatorTest.mapObj(
                2, new LinkedHashSet<>(Collections.singleton(2)),
                1, new LinkedHashSet<>(Collections.singleton(1))));


        Configuration conf = new Configuration();
        VariantTableHelper.setInputTableName(conf, "input");
        VariantTableHelper.setOutputTableName(conf, "output");
        VariantTableHelper.setStudyId(conf, 2);
        this.variantTableHelper = new VariantTableHelper(conf);

        variantConverter = new HBaseAlleleCountsToVariantConverter(this.variantTableHelper, this.singleStudyConfiguration);
        this.variantConverter.setReturnSampleIds(Arrays.asList(1, 2));

        this.calculator = new HBaseAlleleCalculator("2", HBaseAlleleCalculatorTest.mapObj("S2", 2, "S1", 1));
        calculator.addVariant(snv);
        calculator.addVariant(ref);
        AlleleCountPosition validate = validate(new HashSet<Integer>(Arrays.asList(2)), snv,
                Collections.emptyMap(), HBaseAlleleCalculatorTest.mapObj(1, new HashSet<>(Arrays.asList(2))));
        Variant variant = convertBack(this.snv, validate);
        System.out.println("snv = " + snv.getImpl());
        equalsGT("S1", "1", variant);
        equalsGT("S2", "0", variant);
        AlleleStatsCalculator calculator = new AlleleStatsCalculator(Arrays.asList(1, 2));
        VariantStats stats = calculator.calculateStats(validate, new HashSet<>(Arrays.asList(1, 2)), variant);
        System.out.println("stats = " + stats);
        assertEquals(1, stats.getRefAlleleCount().intValue());
        assertEquals(1, stats.getAltAlleleCount().intValue());

    }

    public void equalsGT(String sample, String gt, Variant actual) {
        assertEquals(gt, actual.getStudy("2").getSampleData(sample ,"GT"));
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

        Map<Integer, Set<Integer>> actual = ObjectUtils.firstNonNull(AlleleCombinerTest.convertListToSet(varCount.getReference()), Collections.emptyMap());

        System.out.println("expected = " + expectedReference);
        System.out.println("actual = " + actual);
        assertEquals(expectedReference, actual);
        return varCount;
    }


    public Variant convertBack(Variant input, AlleleCountPosition validate) {
        Variant variant = new Variant(input.getChromosome(), input.getStart(), input.getEnd(), input.getReference(), input.getAlternate(), input.getStrand());
        variant.setType(input.getType());
        variant = this.variantConverter.doConvert(variant, validate);
        System.out.println("variant = " + variant.getImpl());
        return variant;
    }

}