package diva.genome.storage.hbase.allele.stats;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static diva.genome.storage.hbase.VariantHbaseUtil.*;
import static diva.genome.storage.hbase.allele.count.position.AbstractAlleleCalculator.NO_CALL;

/**
 * Created by mh719 on 12/02/2017.
 */
public class VariantTypeSummaryMapper extends AbstractHBaseMapReduce<NullWritable, NullWritable> {

    public static final String ALL = "all";
    public static final String OPR_0_8 = "OPR >= 0.8";
    public static final String OPR_0_9 = "OPR >= 0.9";
    public static final String OPR_0_95 = "OPR >= 0.95";
    private volatile HBaseToAlleleCountConverter alleleCountConverter;
    private byte[] studiesRow;
    private int indexedSampleSize;
    private Map<VariantType, Map<String, Long>> typeCutoffCount = new HashMap<>();

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        this.studiesRow = this.getHelper().generateVariantRowKey("_METADATA", 0);
        // Set samples to fill
        indexedSampleSize = StudyConfiguration.getIndexedSamples(this.getStudyConfiguration()).keySet().size();
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
    }

    public void setAlleleCountConverter(HBaseToAlleleCountConverter alleleCountConverter) {
        this.alleleCountConverter = alleleCountConverter;
    }

    public void setIndexedSampleSize(int indexedSampleSize) {
        this.indexedSampleSize = indexedSampleSize;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.typeCutoffCount.computeIfAbsent(VariantType.INSERTION, k -> new HashMap<>());
        this.typeCutoffCount.computeIfAbsent(VariantType.DELETION, k -> new HashMap<>());
        this.typeCutoffCount.computeIfAbsent(VariantType.MNV, k -> new HashMap<>());
        this.typeCutoffCount.computeIfAbsent(VariantType.SNV, k -> new HashMap<>());
        super.run(context);
        this.typeCutoffCount.forEach((type, map) -> map.forEach((key, count) ->
            context.getCounter(type.toString(), key).increment(count)));
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        if(!Bytes.startsWith(value.getRow(), this.studiesRow)) {
            try {
                this.getLog().info("Convert Variant ...");
                Variant variant = inferAndSetType(this.getHelper().extractVariantFromVariantRowKey(value.getRow()));
                VariantType type = variant.getType();
                context.getCounter("OPENCGA.HBASE", "variants-converted").increment(1L);
                this.getLog().info("Extract Allele counts ...");
                double opr = calculateOpr(value);
                boolean pass08 = opr >= 0.8;
                boolean pass09 = opr >= 0.9;
                boolean pass095 = opr >= 0.95;

                this.getLog().info("Update counts ...");
                Map<String, Long> countMap = this.typeCutoffCount.computeIfAbsent(type, k -> new HashMap<>());
                countMap.put(ALL, countMap.getOrDefault(ALL, 0L) + 1);

                if (pass08) {
                    countMap.put(OPR_0_8, countMap.getOrDefault(OPR_0_8, 0L) + 1);
                }
                if (pass09) {
                    countMap.put(OPR_0_9, countMap.getOrDefault(OPR_0_9, 0L) + 1);
                }
                if (pass095) {
                    countMap.put(OPR_0_95, countMap.getOrDefault(OPR_0_95, 0L) + 1);
                }
            } catch (IllegalStateException var16) {
                throw new IllegalStateException("Problem with row [hex:" + Bytes.toHex(key.copyBytes()) + "]", var16);
            }
        }
    }

    public double calculateOpr(Result value) {
        AlleleCountPosition count = this.alleleCountConverter.convert(value);
        Integer noCall = 0;
        if (count.getReference().containsKey(NO_CALL)) {
            noCall = count.getReference().get(NO_CALL).size();
        }
        Integer callCount = this.indexedSampleSize - noCall;
        Integer passCount = this.indexedSampleSize - count.getNotPass().size();
        double passRate = passCount.doubleValue() / this.indexedSampleSize;
        double callRate = callCount.doubleValue() / this.indexedSampleSize;
        return passRate * callRate;
    }
}
