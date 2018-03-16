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

import vilmaa.genome.analysis.models.variant.stats.HBaseToVariantStatisticsConverter;
import vilmaa.genome.analysis.models.variant.stats.VariantStatistics;
import vilmaa.genome.storage.hbase.VariantHbaseUtil;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.stats.AnalysisStatsMapper;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiConsumer;

/**
 * (Re-)Calculates {@link VariantStatistics} for a list of cohorts. Hardy-Weinberg is optional and can be recalculated separately.
 * Created by mh719 on 01/02/2017.
 */
public class AlleleStatsMapper extends AnalysisStatsMapper {


    private AlleleStatsCalculator alleleStatsCalculator;
    private volatile HBaseToVariantStatisticsConverter statsConverter;
    private Map<String, Set<Integer>> cohortSets;
    private HBaseToAlleleCountConverter alleleCountConverter;
    private volatile boolean calcHwe = false;
    private volatile boolean calcStats = false;
    private final AtomicInteger studyId = new AtomicInteger();
    private final Map<String, Integer> cohortNameToId = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.studyId.set(getHelper().getStudyId());
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
        alleleStatsCalculator = new AlleleStatsCalculator(StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).values());
        
        String[] configCohortsArr = context.getConfiguration().getStrings(AlleleTableStatsDriver.CONFIG_STORAGE_STATS_COHORTS);
        Set<String> confCohorts = new HashSet<>();
        if (null != configCohortsArr) {
            getLog().info("Found stats cohorts: {}", configCohortsArr);
            confCohorts.addAll(Arrays.asList(configCohortsArr));
        }
        if (confCohorts.isEmpty()) {
            // add all as default
            confCohorts.addAll(this.getStudyConfiguration().getCohortIds().keySet());
        }

        this.cohortSets = new HashMap<>();
        Set<Integer> cohortIds = new HashSet<>();
        this.getStudyConfiguration().getCohortIds().forEach((cohort, cid) -> {
            if (this.getStudyConfiguration().getCohorts().containsKey(cid)
                    && confCohorts.contains(cohort)) {
                Set<Integer> ids = this.getStudyConfiguration().getCohorts().get(cid);
                if (null != ids && !ids.isEmpty()) {
                    this.cohortSets.put(cohort, ids);
                    this.cohortNameToId.put(cohort, cid);
                    cohortIds.add(cid);
                }
            }
        });
        getLog().info("Use Cohorts ", this.cohortSets.keySet());
        if (this.cohortSets.isEmpty()) {
            throw new IllegalStateException("No cohort selected to calculate stats for!!!");
        }

        calcHwe = context.getConfiguration().getBoolean(AlleleTableStatsDriver.CONFIG_STORAGE_STATS_HWE, true);
        getLog().info("Calc HWE set to {}", calcHwe);

        calcStats = context.getConfiguration().getBoolean(AlleleTableStatsDriver.CONFIG_STORAGE_STATS_CALC, true);
        getLog().info("Calc stats set to {}", calcStats);

        if (!calcHwe && !calcStats) {
            throw new IllegalStateException("Invalid options - at least one (stat / opr) has to be true!!!");
        }
        this.alleleStatsCalculator.setCalculateHardyWeinberg(calcHwe);
        statsConverter = new HBaseToVariantStatisticsConverter(getHelper(), getHelper().getColumnFamily(), getHelper().getStudyId(), cohortIds);

    }

    private volatile Result currValue;

    @Override
    protected Variant convert(Result value) {
        this.currValue = value;
        return VariantHbaseUtil.inferAndSetType(getHelper().extractVariantFromVariantRowKey(value.getRow())); // dummy variant
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        super.map(key, value, context); // better access for testing
    }

    @Override
    protected List<VariantStatsWrapper> calculateStats(Variant variant) {
        if (null == currValue) {
            return Collections.emptyList();
        }
        AlleleCountPosition counts = this.alleleCountConverter.convert(currValue);
        Map<Integer, Map<Integer, VariantStatistics>> convert;
        if (!calcStats) {
            convert = statsConverter.convert(currValue);
        } else {
            convert = Collections.emptyMap();
        }
        int sid = studyId.get();
        Map<String, VariantStats> statsMap = new HashMap<>(this.cohortSets.size());
        cohortSets.forEach((cohort, ids) -> {
            VariantStatistics variantStats = null;
            if (convert.containsKey(sid) && convert.get(sid).containsKey(cohort)) {
                variantStats = convert.get(sid).get(cohort);
            }
            if (null == variantStats) {
                variantStats = this.alleleStatsCalculator.calculateStats(counts, ids, variant);
            } else if (calcHwe) {
                variantStats.setHw(AlleleStatsCalculator.calcHardyWeinberg(variantStats.getGenotypesCount()));
            }
            statsMap.put(cohort, variantStats);
        });
        return Collections.singletonList(new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), statsMap));
    }

    @Override
    protected Put convertToPut(VariantStatsWrapper annotation) {
        AtomicReference<Put> put = new AtomicReference<>();
        put.set(super.convertToPut(annotation));

        if ( Objects.isNull(put.get())) {
            put.set(new Put(this.currValue.getRow()));
        }
        annotation.getCohortStats().forEach((cohort, stats) -> {
            Integer cohortId = cohortNameToId.get(cohort);
            if (!(stats instanceof VariantStatistics)) {
                throw new IllegalStateException("Expected VariantStatistics class but got " + stats.getClass());
            }
            VariantStatistics vstats = (VariantStatistics) stats;
            BiConsumer<PhoenixHelper.Column, Float> submit = (column, value) -> put.get().addColumn
                    (getHelper().getColumnFamily(), column.bytes(), column.getPDataType().toBytes(value));
            nonNull(vstats.getOverallPassRate(), AlleleTablePhoenixHelper.getOprColumn(studyId.get(), cohortId), submit);
            nonNull(vstats.getCallRate(), AlleleTablePhoenixHelper.getCallRateColumn(studyId.get(), cohortId), submit);
            nonNull(vstats.getPassRate(), AlleleTablePhoenixHelper.getPassRateColumn(studyId.get(), cohortId), submit);

        });
        return put.get();
    }

    protected <T> void nonNull(T value, PhoenixHelper.Column column, BiConsumer<PhoenixHelper.Column, T> consumer) {
        if (null != value) {
            consumer.accept(column, value);
        }
    }
}
