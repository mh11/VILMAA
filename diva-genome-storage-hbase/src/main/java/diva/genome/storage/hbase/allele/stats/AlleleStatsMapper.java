package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.stats.AnalysisStatsMapper;

import java.io.IOException;
import java.util.*;

/**
 * Created by mh719 on 01/02/2017.
 */
public class AlleleStatsMapper extends AnalysisStatsMapper {

    private AlleleStatsCalculator alleleStatsCalculator;
    private Map<String, Set<Integer>> cohortSets;
    private HBaseToAlleleCountConverter alleleCountConverter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
        alleleStatsCalculator = new AlleleStatsCalculator(StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).values());
        this.cohortSets = new HashMap<>();
        this.getStudyConfiguration().getCohortIds().forEach((cohort, cid) -> {
            if (this.getStudyConfiguration().getCohorts().containsKey(cid)) {
                Set<Integer> ids = this.getStudyConfiguration().getCohorts().get(cid);
                if (null != ids && !ids.isEmpty()) {
                    this.cohortSets.put(cohort, ids);
                }
            }
        });
    }

    private volatile Result currValue;

    @Override
    protected Variant convert(Result value) {
        this.currValue = value;
        return getHelper().extractVariantFromVariantRowKey(value.getRow()); // dummy variant
    }

    @Override
    protected List<VariantStatsWrapper> calculateStats(Variant variant) {
        if (null == currValue) {
            return Collections.emptyList();
        }
        Map<String, VariantStats> statsMap = new HashMap<>(this.cohortSets.size());
        AlleleCountPosition counts = this.alleleCountConverter.convert(currValue);
        cohortSets.forEach((cohort, ids) -> {
            VariantStats variantStats = this.alleleStatsCalculator.calculateStats(counts, ids, variant);
            statsMap.put(cohort, variantStats);
        });
        this.currValue = null; // done with processing.
        return Collections.singletonList(new VariantStatsWrapper(variant.getChromosome(), variant.getStart(), statsMap));
    }
}
