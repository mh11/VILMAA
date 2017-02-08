package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.exporters.AnalysisToFileMapper;

import java.io.IOException;

/**
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableToVariantMapper extends AnalysisToFileMapper {

    private volatile HBaseAlleleCountsToVariantConverter countsToVariantConverter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        countsToVariantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        this.countsToVariantConverter.setParseStatistics(true);
        this.countsToVariantConverter.setParseAnnotations(true);
        this.countsToVariantConverter.setReturnSamples(this.returnedSamples);
    }

    @Override
    protected Variant convertToVariant(Result value) {
        return this.countsToVariantConverter.convert(value);
    }
}
