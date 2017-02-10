package diva.genome.storage.hbase.allele.fix;

import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import diva.genome.storage.hbase.allele.count.converter.AlleleCountToHBaseAppendGroupedConverter;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.function.Consumer;

/**
 * Created by mh719 on 09/02/2017.
 */
public class FromPhoenixToProtoMapper extends AbstractVariantTableMapReduce {
    private byte[] studiesRow;
    protected HBaseToAlleleCountConverter converter;
    private AlleleCountToHBaseAppendGroupedConverter groupedConverter;

    protected volatile List<Pair<Variant, Result>> positionBuffer = new ArrayList<>();

    public void setGroupedConverter(AlleleCountToHBaseAppendGroupedConverter groupedConverter) {
        this.groupedConverter = groupedConverter;
    }

    public void setConverter(HBaseToAlleleCountConverter converter) {
        this.converter = converter;
    }

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        converter = new HBaseToAlleleCountConverter();
        groupedConverter = new AlleleCountToHBaseAppendGroupedConverter(getHelper().getColumnFamily());
    }

    @Override
    public void run(Context context) throws IOException, InterruptedException {
        this.setup(context);

        String chromosome = "-1";
        int referencePosition = -1;
        Consumer<List<Append>> submitFunction = appends -> {
            try {
                context.getCounter("OPENCGA", "transfer-append").increment(1);
                getHelper().getHBaseManager().act(getHelper().getOutputTableAsString(), (tab -> {
                    try {
                        Object[] results = new Object[appends.size()];
                        tab.batch(appends, results);
                    } catch (InterruptedException e) {
                        throw new IllegalStateException(e);
                    }
                }));
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
                int nextPos = groupedConverter.calcPosition(variant.getStart());
                if (referencePosition != nextPos) {
                    flushBuffer(chromosome, submitFunction);
                }
                chromosome = variant.getChromosome();
                referencePosition = variant.getStart();
                addToBuffer(new ImmutablePair<>(variant, result));
            }
            // end
            flushBuffer(chromosome, submitFunction);
        } catch (Exception e) {
            throw new IllegalStateException("Something went wrong during transfer", e);
        } finally {
            this.cleanup(context);
        }
    }

    public void flushBuffer(String chromosome, Consumer<List<Append>> submitFunction) {
        Map<Integer, AlleleCountPosition> refMap = new HashMap<>();
        Map<Integer, Map<String, AlleleCountPosition>> altMap = new HashMap<>();

        this.positionBuffer.forEach(pair -> {
            Variant variant = pair.getLeft();
            Result result = pair.getRight();
            switch (variant.getType()) {
                case NO_VARIATION:
                    refMap.put(variant.getStart(), this.converter.convert(result));
                    break;
                case INDEL:
                case INSERTION:
                case DELETION:
                case SNV:
                case SNP:
                case MNV:
                case MNP:
                    Map<String, AlleleCountPosition> map = altMap.computeIfAbsent(variant.getStart(), x -> new HashMap<>());
                    AlleleCountPosition count = this.converter.convert(result);
                    String varId = variant.getReference() + "_" + variant.getAlternate();
                    map.put(varId, count);
                    break;
                default:
                    throw new IllegalStateException("Type not supported: " + variant.getType() + " for " + variant);
            }
        });

        Collection<Append> appends = groupedConverter.convert(chromosome, refMap, altMap);
        this.positionBuffer.clear();
        if (null == appends || appends.isEmpty()) {
            return;
        }
        submitFunction.accept(new ArrayList<>(appends));
    }

    public void addToBuffer(Pair<Variant, Result> data) {
        this.positionBuffer.add(data);
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
        // do nothing
    }

}
