package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.converter.HBaseAppendGroupedToAlleleCountConverter;
import diva.genome.storage.hbase.allele.count.position.HBaseAlleleTransfer;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 08/02/2017.
 */
public class HbaseGroupedAlleleTransferMapper  extends AbstractVariantTableMapReduce {
    private byte[] studiesRow;
    protected volatile Map<Integer, Map<Integer, Integer>> deletionEnds = new HashMap<>();
    protected AlleleCountToHBaseConverter converter;
    protected HBaseAppendGroupedToAlleleCountConverter groupedConverter;
    private HBaseAlleleTransfer hBaseAlleleTransfer;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        HashSet<Integer> sampleIds = new HashSet<>(this.getIndexedSamples().values());
        String study = Integer.valueOf(getStudyConfiguration().getStudyId()).toString();
        converter = new AlleleCountToHBaseConverter(getHelper().getColumnFamily(), study);
        groupedConverter = new HBaseAppendGroupedToAlleleCountConverter(getHelper().getColumnFamily());
        hBaseAlleleTransfer = new HBaseAlleleTransfer(sampleIds);
    }

    public void setStudiesRow(byte[] studiesRow) {
        this.studiesRow = studiesRow;
    }

    protected void clearRegionOverlap() {
        deletionEnds.clear();
    }

    protected boolean isMetaRow(byte[] rowKey) {
        return Bytes.startsWith(rowKey, this.studiesRow);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        // don't use
    }

    @Override
    protected void doMap(VariantMapReduceContext variantMapReduceContext) throws IOException, InterruptedException {
        // don't use
    }

    protected Variant extractVariant(String chromosome, Integer position, String varId) {
        String[] split = varId.split("_", 2);
        String ref = split[0];
        String alt = split[1];
        return new Variant(chromosome, position, ref, alt);
    }

    protected List<Pair<AlleleCountPosition, Variant>> extractToVariants(String chrom, Integer pos,
                                                                         Map<String, AlleleCountPosition> alts)  {
        List<Pair<AlleleCountPosition, Variant>> pairs = new ArrayList<>();
        alts.forEach((varid, cnt) -> pairs.add(new ImmutablePair<>(cnt, extractVariant(chrom, pos, varid))));
        return pairs;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        context.getCounter("OPENCGA", "RUN_START").increment(1);
        // buffer
        String chromosome = "-1";
        clearRegionOverlap();

        Consumer<Put> submitFunction = put -> {
            try {
                context.getCounter("OPENCGA", "transfer-put").increment(1);
                context.write(new ImmutableBytesWritable(put.getRow()), put);
            } catch (Exception e) {
                getLog().error("Problems with submitting", e);
                throw new IllegalStateException(e);
            }
        };

        try {
            while (context.nextKeyValue()) {
                ImmutableBytesWritable currentKey = context.getCurrentKey();

                Result result = context.getCurrentValue();
                if (isMetaRow(result.getRow())) {
                    context.getCounter("OPENCGA", "META_ROW").increment(1);
                    continue;
                }
                Pair<String, Integer> pair = groupedConverter.extractRegion(currentKey.copyBytes());
                if (!pair.getLeft().equals(chromosome)) {
                    hBaseAlleleTransfer.resetNewChromosome();
                    chromosome = pair.getLeft();
                    clearRegionOverlap();
                }

                Map<Integer, Pair<AlleleCountPosition, Map<String, AlleleCountPosition>>> regionData =
                        groupedConverter.convert(result);

                List<Integer> positions = new ArrayList<>(regionData.keySet());
                Collections.sort(positions); // sort positions

                for (Integer position : positions) {
                    checkDeletionOverlapMap(position);
                    Pair<AlleleCountPosition, Map<String, AlleleCountPosition>> refAndAlts =
                            regionData.get(position);
                    List<Pair<AlleleCountPosition, Variant>> variants = extractToVariants(pair.getLeft(), position,
                            refAndAlts.getRight());
                    hBaseAlleleTransfer.process(refAndAlts.getLeft(), variants, (v, a) -> toPut(v, a));
                }

            }
            getLog().info("Done ...");
        } finally {
            this.cleanup(context);
        }
    }

    protected void checkDeletionOverlapMap(Integer start) {
        if (deletionEnds.isEmpty()) {
            return;
        }
        List<Integer> old = deletionEnds.keySet().stream().filter(i -> i < start).collect(Collectors.toList());
        old.forEach(o -> deletionEnds.remove(o));
    }

    private Put toPut(Variant variant, AlleleCountPosition to) {
        return this.converter.convertPut(variant.getChromosome(), variant.getStart(),
                variant.getReference(), variant.getAlternate(), variant.getType(), to);
    }

    public void setConverter(AlleleCountToHBaseConverter converter) {
        this.converter = converter;
    }

    public void setGroupedConverter(HBaseAppendGroupedToAlleleCountConverter groupedConverter) {
        this.groupedConverter = groupedConverter;
    }
}
