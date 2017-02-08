package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.stats.AnalysisStatsMapper;

import java.io.IOException;

/**
 * Created by mh719 on 01/02/2017.
 */
public class AlleleStatsMapper extends AnalysisStatsMapper {

    private HBaseAlleleCountsToVariantConverter alleleCountsToVariantConverter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.alleleCountsToVariantConverter =
                new HBaseAlleleCountsToVariantConverter(this.getHelper(), getStudyConfiguration());
        this.alleleCountsToVariantConverter.setParseAnnotations(false);
        this.alleleCountsToVariantConverter.setParseStatistics(false);
        // Set samples to fill
        this.alleleCountsToVariantConverter.setReturnSamples(
                StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).keySet());
    }

    @Override
    protected Variant convert(Result value) {
        return alleleCountsToVariantConverter.convert(value);
    }

}
