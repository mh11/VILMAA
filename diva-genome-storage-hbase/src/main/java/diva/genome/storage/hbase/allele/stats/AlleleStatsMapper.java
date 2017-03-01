package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.AbstractHBaseAlleleCountsConverter;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import diva.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
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
import java.util.function.Consumer;

import static diva.genome.storage.hbase.allele.stats.AlleleTableStatsDriver.*;

/**
 * Created by mh719 on 01/02/2017.
 */
public class AlleleStatsMapper extends AnalysisStatsMapper {

    private AlleleStatsCalculator alleleStatsCalculator;
    private Map<String, Set<Integer>> cohortSets;
    private HBaseToAlleleCountConverter alleleCountConverter;
    private volatile boolean calcOpr = false;
    private volatile boolean calcStats = false;
    private final AtomicInteger studyId = new AtomicInteger();
    private final Map<String, Float> cohortOpr = new HashMap<>();
    private final Map<String, Integer> cohortNameToId = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.studyId.set(getHelper().getStudyId());
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
        alleleStatsCalculator = new AlleleStatsCalculator(StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).values());
        
        String[] configCohortsArr = context.getConfiguration().getStrings(CONFIG_STORAGE_STATS_COHORTS);
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
        this.getStudyConfiguration().getCohortIds().forEach((cohort, cid) -> {
            if (this.getStudyConfiguration().getCohorts().containsKey(cid)
                    && confCohorts.contains(cohort)) {
                Set<Integer> ids = this.getStudyConfiguration().getCohorts().get(cid);
                if (null != ids && !ids.isEmpty()) {
                    this.cohortSets.put(cohort, ids);
                    this.cohortNameToId.put(cohort, cid);
                }
            }
        });
        getLog().info("Use Cohorts ", this.cohortSets.keySet());
        if (this.cohortSets.isEmpty()) {
            throw new IllegalStateException("No cohort selected to calculate stats for!!!");
        }

        calcOpr = context.getConfiguration().getBoolean(CONFIG_STORAGE_STATS_OPR, true);
        getLog().info("Calc OPR set to {}", calcOpr);

        calcStats = context.getConfiguration().getBoolean(CONFIG_STORAGE_STATS_CALC, true);
        getLog().info("Calc stats set to {}", calcStats);

        if (!calcOpr && !calcStats) {
            throw new IllegalStateException("Invalid options - at least one (stat / opr) has to be true!!!");
        }
    }

    private volatile Result currValue;

    @Override
    protected Variant convert(Result value) {
        cohortOpr.clear();
        this.currValue = value;
        return getHelper().extractVariantFromVariantRowKey(value.getRow()); // dummy variant
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

        Map<String, VariantStats> statsMap = new HashMap<>(this.cohortSets.size());
        cohortSets.forEach((cohort, ids) -> {
            Consumer<AlleleCountPosition> oprFunction = (a) -> {}; // do nothing
            if (calcOpr) {
                // function to consume filtered allele count object;
                oprFunction = (a) -> {
                    Map<String, String> attr = AbstractHBaseAlleleCountsConverter.calculatePassCallRates(a, ids.size());
                    Float opr = Float.valueOf(attr.get("OPR"));
                    cohortOpr.put(cohort, opr);
                };
            }
            if (calcStats) {
                VariantStats variantStats = this.alleleStatsCalculator.calculateStats(counts, ids, variant, oprFunction);
                statsMap.put(cohort, variantStats);
            } else if (calcOpr){
                // Filter Counts based on ID list
                AlleleCountPosition row = new AlleleCountPosition(counts, ids);
                // calc OPR
                oprFunction.accept(row);
            }
        });
        return Collections.singletonList(new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), statsMap));
    }

    @Override
    protected Put convertToPut(VariantStatsWrapper annotation) {
        AtomicReference<Put> put = new AtomicReference<>();
        if (calcStats) {
            put.set(super.convertToPut(annotation));
        }
        if ( Objects.isNull(put.get()) && calcOpr) {
            put.set(new Put(this.currValue.getRow()));
        }
        if (calcOpr) {
            this.cohortOpr.forEach((cohort, opr) -> {
                Integer cohortId = cohortNameToId.get(cohort);
                PhoenixHelper.Column column = AlleleTablePhoenixHelper.getOprColumn(studyId.get(), cohortId);
                put.get().addColumn(getHelper().getColumnFamily(), column.bytes(), column.getPDataType().toBytes(opr));
            });
        }
        return put.get();
    }
}
