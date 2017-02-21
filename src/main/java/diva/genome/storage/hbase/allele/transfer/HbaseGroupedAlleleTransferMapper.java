package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.converter.HBaseAppendGroupedToAlleleCountConverter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 08/02/2017.
 */
public class HbaseGroupedAlleleTransferMapper  extends AbstractVariantTableMapReduce {
    private byte[] studiesRow;
    protected volatile Map<Integer, Map<Integer, Integer>> deletionEnds = new HashMap<>();
    protected volatile AlleleCombiner alleleCombiner;
    protected AlleleCountToHBaseConverter converter;
    protected HBaseAppendGroupedToAlleleCountConverter groupedConverter;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        alleleCombiner = new AlleleCombiner(new HashSet<>(this.getIndexedSamples().values()));
        String study = Integer.valueOf(getStudyConfiguration().getStudyId()).toString();
        converter = new AlleleCountToHBaseConverter(getHelper().getColumnFamily(), study);
        groupedConverter = new HBaseAppendGroupedToAlleleCountConverter(getHelper().getColumnFamily());
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

    protected List<Pair<Variant, AlleleCountPosition>> extractToVariants(String chrom, Integer pos,
                                                                         Map<String, AlleleCountPosition> alts)  {
        List<Pair<Variant, AlleleCountPosition>> pairs = new ArrayList<>();
        alts.forEach((varid, cnt) -> pairs.add(new ImmutablePair<>(extractVariant(chrom, pos, varid), cnt)));
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
                    List<Pair<Variant, AlleleCountPosition>> variants = extractToVariants(pair.getLeft(), position,
                            refAndAlts.getRight());
                    processVariants(refAndAlts.getLeft(), variants, submitFunction);
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

    protected void processVariants(AlleleCountPosition refBean, List<Pair<Variant, AlleleCountPosition>> variants,
                                 Consumer<Put> submitFunction) {


        Collections.sort(variants, (a, b) -> {
            int cmp = Integer.compare(
                    Math.abs(a.getLeft().getStart() - a.getLeft().getEnd()),
                    Math.abs(b.getLeft().getStart() - b.getLeft().getEnd()));
            if (cmp == 0) {
                cmp = a.getLeft().getEnd().compareTo(b.getLeft().getEnd());
            }
            if (cmp == 0) {
                cmp = b.getLeft().getLength().compareTo(a.getLeft().getLength());
            }
            return cmp;
        });

        // Create Overlap Index
        Consumer<Pair<Variant, AlleleCountPosition>> registerOverlapFunction = pair -> {
            // process all variants as normal
            Variant variant = pair.getLeft();
            // update indel covered regions
            addRegionOverlapIfRequired(variant, pair.getRight());
        };

        // process data
        Consumer<Pair<Variant, AlleleCountPosition>> combineFunction = pair -> {
            // process all variants as normal
            Variant variant = pair.getLeft();
            Put putNew = newTransfer(variant, refBean, pair.getRight());
            submitFunction.accept(putNew);
        };

        Predicate<Pair<Variant, AlleleCountPosition>> isIndelFunction = p -> {
            Variant var = p.getLeft();
            return var.getType().equals(VariantType.INDEL) && var.getStart() > var.getEnd();
        };
        Predicate<Pair<Variant, AlleleCountPosition>> isNotIndelFunction = i -> !isIndelFunction.test(i);

        // Only INDELs first -> There is no overlap with Deletions starting at same position
        variants.stream().filter(isIndelFunction).forEach(registerOverlapFunction);
        variants.stream().filter(isIndelFunction).forEach(combineFunction);

        // all the others
        variants.stream().filter(isNotIndelFunction).forEach(registerOverlapFunction);
        variants.stream().filter(isNotIndelFunction).forEach(combineFunction);
    }


    protected Put newTransfer(Variant variant, AlleleCountPosition from, AlleleCountPosition to) {
        this.alleleCombiner.combine(variant, from, to, this.deletionEnds);
        return this.converter.convertPut(variant.getChromosome(), variant.getStart(),
                variant.getReference(), variant.getAlternate(), variant.getType(), to);
    }


    protected void addRegionOverlapIfRequired(Variant variant, AlleleCountPosition toBean) {
        BiConsumer<Map<Integer, Map<Integer, Integer>>, AlleleCountPosition> overlapFunction = (map, bean) -> {
            Map<Integer, Integer> endMap = map.computeIfAbsent(variant.getEnd(), f -> new HashMap<>());
            bean.getAlternate().forEach((position, ids) -> ids.forEach(sid -> {
                endMap.put(sid, endMap.getOrDefault(sid, 0) + position);
            }));
        };

        switch (variant.getType()) {
            case SNV:
            case SNP:
                break; // do nothing
            case MNP:
            case MNV:
            case INDEL:
            case INSERTION:
            case DELETION:
                overlapFunction.accept(this.deletionEnds, toBean);
                break;
            default:
                throw new IllegalStateException("Currently not support: " + variant.getType());

        }
    }

    public void setAlleleCombiner(AlleleCombiner alleleCombiner) {
        this.alleleCombiner = alleleCombiner;
    }

    public void setConverter(AlleleCountToHBaseConverter converter) {
        this.converter = converter;
    }

    public void setGroupedConverter(HBaseAppendGroupedToAlleleCountConverter groupedConverter) {
        this.groupedConverter = groupedConverter;
    }
}
