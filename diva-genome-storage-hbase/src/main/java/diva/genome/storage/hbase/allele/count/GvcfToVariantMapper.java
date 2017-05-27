package diva.genome.storage.hbase.allele.count;

import diva.genome.storage.hbase.allele.count.region.AlleleRegionCalculator;
import diva.genome.storage.hbase.allele.count.region.AlleleRegionStore;
import diva.genome.storage.hbase.allele.count.region.HBaseAlleleRegionTransfer;
import diva.genome.util.PointRegion;
import diva.genome.util.Region;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

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
        // get all Variant positions out
        List<Integer> lst =
                new ArrayList<>(store.getVariation(targetRegion).stream().map(e -> e.getStart()).collect(Collectors.toSet()));
        Collections.sort(lst);
        // Iterate from first to last in order
        for (Integer position : lst) {
            this.gethBaseAlleleRegionTransfer().transfer(chromosome, new PointRegion(null, position), store, (var, cnt) -> {
                Put put = this.alleleCountToHBaseConverter.convertPut(
                        var.getChromosome(), var.getStart(), var.getReference(), var.getAlternate(), cnt);
                try {
                    getContext().write(new ImmutableBytesWritable(put.getRow()), put);
                } catch (IOException e) {
                    throw new IllegalStateException("Issue submitting " + var, e);
                } catch (InterruptedException e) {
                    throw new IllegalStateException("Issue submitting " + var, e);
                }
            });
        }
        return Collections.emptyList();
    }

    protected Context getContext() {
        return ctx;
    }
}
