package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Mapper;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;

import javax.lang.model.type.NullType;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;

/**
 * Created by mh719 on 12/02/2017.
 */
public class VariantTypeSummaryMapper extends AbstractHBaseMapReduce<ImmutableBytesWritable, NullType> {

    private HBaseAlleleCountsToVariantConverter alleleCountsToVariantConverter;
    private byte[] studiesRow;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.studiesRow = this.getHelper().generateVariantRowKey("_METADATA", 0);
        this.alleleCountsToVariantConverter =
                new HBaseAlleleCountsToVariantConverter(this.getHelper(), getStudyConfiguration());
        this.alleleCountsToVariantConverter.setParseAnnotations(false);
        this.alleleCountsToVariantConverter.setParseStatistics(false);
        // Set samples to fill
        this.alleleCountsToVariantConverter.setReturnSamples(
                StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).keySet());
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        if(!Bytes.startsWith(value.getRow(), this.studiesRow)) {
            try {
                this.getLog().info("Convert Variant ...");
                Variant variant = this.convert(value);
                context.getCounter("OPENCGA.HBASE", "variants-converted").increment(1L);
                this.getLog().info("Calculate Stats ...");
                String opr = variant.getStudy(this.getHelper().getStudyId() + "").getFiles().get(0).getAttributes()
                        .getOrDefault("OPR", "0");
                boolean pass08 = Double.valueOf(opr) >= 0.8;
                boolean pass09 = Double.valueOf(opr) >= 0.9;
                boolean pass095 = Double.valueOf(opr) >= 0.95;

                context.getCounter(variant.getType().toString(), "all").increment(1);
                if (pass08) {
                    context.getCounter(variant.getType().toString(), "OPR >= 0.8").increment(1);
                }
                if (pass09) {
                    context.getCounter(variant.getType().toString(), "OPR >= 0.9").increment(1);
                }
                if (pass095) {
                    context.getCounter(variant.getType().toString(), "OPR >= 0.95").increment(1);
                }
            } catch (IllegalStateException var16) {
                throw new IllegalStateException("Problem with row [hex:" + Bytes.toHex(key.copyBytes()) + "]", var16);
            }
        }
    }

    protected Variant convert(Result value) {
        return alleleCountsToVariantConverter.convert(value);
    }
}
