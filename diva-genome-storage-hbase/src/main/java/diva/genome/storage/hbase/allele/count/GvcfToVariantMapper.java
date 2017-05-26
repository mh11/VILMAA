package diva.genome.storage.hbase.allele.count;

import diva.genome.storage.hbase.allele.count.region.AlleleRegionCalculator;
import diva.genome.storage.hbase.allele.count.region.AlleleRegionStore;
import diva.genome.storage.hbase.allele.count.region.HBaseAlleleRegionTransfer;
import diva.genome.util.Region;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;

/**
 * Created by mh719 on 25/05/2017.
 */
public class GvcfToVariantMapper extends HbaseTableMapper {

    private HBaseAlleleRegionTransfer hBaseAlleleRegionTransfer;
    private AlleleCountToHBaseConverter alleleCountToHBaseConverter;
    private Context ctx;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        hBaseAlleleRegionTransfer = new HBaseAlleleRegionTransfer(new HashSet<>(getSampleNameToSampleId().values()));
        alleleCountToHBaseConverter = new AlleleCountToHBaseConverter(getHelper().getColumnFamily(), Integer.toString(getHelper().getStudyId()));
    }

    public HBaseAlleleRegionTransfer gethBaseAlleleRegionTransfer() {
        return hBaseAlleleRegionTransfer;
    }

    public AlleleCountToHBaseConverter getAlleleCountToHBaseConverter() {
        return alleleCountToHBaseConverter;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.ctx = context;
        super.run(context);
    }

    @Override
    protected Collection<Append> packageAlleleCounts(String chromosome, String studyId, AlleleRegionCalculator alleleCalculator) {
        AlleleRegionStore store = alleleCalculator.getStore();
        Region targetRegion = store.getTargetRegion();
        this.gethBaseAlleleRegionTransfer().transfer(chromosome, targetRegion, store, (var, cnt) -> {
            Put put = this.alleleCountToHBaseConverter.convertPut(
                    var.getChromosome(), var.getStart(), var.getReference(), var.getAlternate(), cnt);
            try {
                ctx.write(new ImmutableBytesWritable(put.getRow()), put);
            } catch (IOException e) {
                throw new IllegalStateException("Issue submitting " + var, e);
            } catch (InterruptedException e) {
                throw new IllegalStateException("Issue submitting " + var, e);
            }
        });
        return Collections.emptyList();
    }
}
