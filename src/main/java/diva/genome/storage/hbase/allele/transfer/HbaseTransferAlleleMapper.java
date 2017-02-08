package diva.genome.storage.hbase.allele.transfer;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import diva.genome.storage.hbase.allele.transfer.AlleleCombiner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 28/01/2017.
 */
public class HbaseTransferAlleleMapper extends AbstractVariantTableMapReduce {

    private byte[] studiesRow;
    protected volatile Map<Integer, Map<Integer, Integer>> deletionEnds = new HashMap<>();
    protected volatile List<Pair<Result, Variant>> positionBuffer = new ArrayList<>();
    protected volatile AlleleCombiner alleleCombiner;
    protected AlleleCountToHBaseConverter converter;
    protected HBaseToAlleleCountConverter alleleCountConverter;

    public void setAlleleCountConverter(HBaseToAlleleCountConverter alleleCountConverter) {
        this.alleleCountConverter = alleleCountConverter;
    }

    public void setAlleleCombiner(AlleleCombiner alleleCombiner) {
        this.alleleCombiner = alleleCombiner;
    }

    public void setConverter(AlleleCountToHBaseConverter converter) {
        this.converter = converter;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        alleleCombiner = new AlleleCombiner(new HashSet<>(this.getIndexedSamples().values()));

        String study = Integer.valueOf(getStudyConfiguration().getStudyId()).toString();
        converter = new AlleleCountToHBaseConverter(getHelper().getColumnFamily(), study);
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
    }

    public void setStudiesRow(byte[] studiesRow) {
        this.studiesRow = studiesRow;
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        context.getCounter("OPENCGA", "RUN_START").increment(1);
        // buffer
        String chromosome = "-1";
        Integer referencePosition = -1;
        Result referenceResult = null;
        AlleleCountPosition refBean = null;
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
                Variant variant = getHelper().extractVariantFromVariantRowKey(result.getRow());
                if (variant.getStart() > referencePosition && !positionBuffer.isEmpty()) {
                    getLog().info("Process buffer of {} for position ... ", positionBuffer.size(), referencePosition);
                    context.getCounter("OPENCGA", "BUFFER-process").increment(1);
                    processBuffer(refBean, submitFunction);
                    positionBuffer.clear();
                }
                if (!StringUtils.equals(chromosome, variant.getChromosome())) {
                    referencePosition = -1;
                    clearRegionOverlap();
                    chromosome = variant.getChromosome();
                }
                checkDeletionOverlapMap(variant.getStart());
                if (variant.getType().equals(VariantType.NO_VARIATION)) {
                    context.getCounter("OPENCGA", "NO_VARIATION").increment(1);
                    referencePosition = variant.getStart();
                    referenceResult = result;
                    refBean = null;
                    continue;
                }
                // if actual variant
                if (null == referencePosition || !referencePosition.equals(variant.getStart())) {
                    context.getCounter("OPENCGA", "START-BLOCK_SCAN").increment(1);
                    // should only happen at the start of a split block.
                    referenceResult = queryForRefernce(variant);
                }
                if (refBean == null) {
                    context.getCounter("OPENCGA", "reference_parse").increment(1);
                    refBean = this.alleleCountConverter.convert(referenceResult);
                }
                this.positionBuffer.add(new ImmutablePair<>(result, variant));
            }
            getLog().info("Clear buffer ...");
            if (!positionBuffer.isEmpty()) {
                context.getCounter("OPENCGA", "BUFFER-process").increment(1);
                processBuffer(refBean, submitFunction);
                positionBuffer.clear();
            }
            getLog().info("Done ...");
        } catch (SQLException e) {
            throw new IllegalStateException("Something went wrong during transfer", e);
        } finally {
            this.cleanup(context);
        }
    }

    protected void clearRegionOverlap() {
        deletionEnds.clear();
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

    protected void processBuffer(AlleleCountPosition refBean, Consumer<Put> submit)
            throws IOException, InterruptedException, SQLException {

        Collections.sort(this.positionBuffer, (a, b) -> {
            int cmp = Integer.compare(
                    Math.abs(a.getRight().getStart() - a.getRight().getEnd()),
                    Math.abs(b.getRight().getStart() - b.getRight().getEnd()));
            if (cmp == 0) {
                cmp = a.getRight().getEnd().compareTo(b.getRight().getEnd());
            }
            if (cmp == 0) {
                cmp = b.getRight().getLength().compareTo(a.getRight().getLength());
            }
            return cmp;
        });

        // Create Overlap Index
        Consumer<Pair<Result, Variant>> registerOverlapFunction = pair -> {
            AlleleCountPosition toBean = this.alleleCountConverter.convert(pair.getLeft());
            // process all variants as normal
            Variant variant = pair.getRight();
            // update indel covered regions
            addRegionOverlapIfRequired(variant, toBean);
        };

        // process data
        Consumer<Pair<Result, Variant>> combineFunction = pair -> {
            AlleleCountPosition toBean = this.alleleCountConverter.convert(pair.getLeft());
            // process all variants as normal
            Variant variant = pair.getRight();
            Put putNew = newTransfer(variant, refBean, toBean);
            submit.accept(putNew);
        };

        Predicate<Pair<Result, Variant>> isIndelFunction = p -> {
            Variant var = p.getRight();
            return var.getType().equals(VariantType.INDEL) && var.getStart() > var.getEnd();
        };
        Predicate<Pair<Result, Variant>> isNotIndelFunction = i -> !isIndelFunction.test(i);

        // Only INDELs first -> There is no overlap with Deletions starting at same position
        this.positionBuffer.stream().filter(isIndelFunction).forEach(registerOverlapFunction);
        this.positionBuffer.stream().filter(isIndelFunction).forEach(combineFunction);

        // all the others
        this.positionBuffer.stream().filter(isNotIndelFunction).forEach(registerOverlapFunction);
        this.positionBuffer.stream().filter(isNotIndelFunction).forEach(combineFunction);
    }

    private Put newTransfer(Variant variant, AlleleCountPosition from, AlleleCountPosition to) {
        this.alleleCombiner.combine(variant, from, to, this.deletionEnds);
        return this.converter.convertPut(variant.getChromosome(), variant.getStart(),
                variant.getReference(), variant.getAlternate(), variant.getType(), to);
    }

    protected void checkDeletionOverlapMap(Integer start) {
        if (deletionEnds.isEmpty()) {
            return;
        }
        List<Integer> old = deletionEnds.keySet().stream().filter(i -> i < start).collect(Collectors.toList());
        old.forEach(o -> deletionEnds.remove(o));
    }

    protected Result queryForRefernce(Variant variant) throws IOException {
        getLog().info("Query reference variant for ", variant);
        Get get = new Get(AlleleCountToHBaseConverter.buildRowKey(
                variant.getChromosome(), variant.getStart(), StringUtils.EMPTY, Allele.NO_CALL_STRING));
        Result res = getHelper().getHBaseManager().act(getHelper().getOutputTable(), table -> table.get(get));
        if (res.isEmpty()) {
            throw new IllegalStateException(
                    "No entry found for reference " + variant.getChromosome() + ":" + variant.getStart());
        }
        return res;
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

}
