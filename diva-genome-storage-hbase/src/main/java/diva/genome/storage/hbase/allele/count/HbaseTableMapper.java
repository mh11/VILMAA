package diva.genome.storage.hbase.allele.count;

import diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator;
import diva.genome.storage.hbase.allele.count.region.AlleleRegionCalculator;
import diva.genome.storage.hbase.allele.count.region.AlleleRegionStoreToHBaseAppendConverter;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper.*;

/**
 * Created by mh719 on 22/01/2017.
 */
public class HbaseTableMapper extends AbstractVariantTableMapReduce {
    public static final String END = "END";
    public static final String DIVA_GENOME_STORAGE_ALLELE_COUNT_FIXEND = "diva.genome.storage.allele.count.fixend";
    private Map<String, Integer> sampleNameToSampleId;
    private volatile ExecutorService submitterPool;
    private final BlockingDeque<Collection<Append>> submitQueue = new LinkedBlockingDeque<>(2); // 1.5 x slice length
    private final AtomicBoolean asyncPut = new AtomicBoolean(false);
    private final AtomicBoolean asyncFinished = new AtomicBoolean(false);
    private volatile Future<String> submitFuture;
    private volatile AlleleRegionStoreToHBaseAppendConverter converter;
    private boolean fixEndPosition = false;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        setSampleNameToSampleId(getStudyConfiguration().getSampleIds());
        getLog().info("FORCE Parallel to FALSE.");
        this.getResultConverter().setParallel(false); // Parallel not tested!!!
        this.asyncPut.set(context.getConfiguration().getBoolean(ARCHIVE_GET_BATCH_ASYNC, false));
        getLog().info("Async retrieval of archive batches: {}", this.asyncPut.get());
        if (this.asyncPut.get()) {
            setupAsyncQueue(
                    f -> context.getCounter(COUNTER_GROUP_NAME, "async-received").increment(f.size()),
                    f -> context.getCounter(COUNTER_GROUP_NAME, "async-submitted").increment(f.size()));
        }
        converter = new AlleleRegionStoreToHBaseAppendConverter(getHelper().getColumnFamily(), getHelper().getStudyId());
        this.fixEndPosition = context.getConfiguration().getBoolean(DIVA_GENOME_STORAGE_ALLELE_COUNT_FIXEND, false);
    }

    public void setAsyncPut(boolean asyncPut) {
        this.asyncPut.set(asyncPut);
    }

    public void setConverter(AlleleRegionStoreToHBaseAppendConverter converter) {
        this.converter = converter;
    }

    @Override
    protected void cleanup(Context context) throws IOException, InterruptedException {
        cleanupAsyncQueue();
        super.cleanup(context);
    }

    protected void cleanupAsyncQueue() {
        if (null != this.submitterPool) {
            try {
                this.submitterPool.shutdown();
                this.asyncFinished.set(true); // set shutdown when queue is empty
                this.submitQueue.offer(Collections.emptyList()); // trigger wake-up of queue
                try {
                    submitFuture.get(10L, TimeUnit.MINUTES); // Wait for 10 min (to clear queue)
                } catch (Exception e) {
                    getLog().error("Problems waiting for Submit future ... ", e);
                }
                if (!submitFuture.isDone()) {
                    this.submitFuture.cancel(true);
                }
                this.submitterPool.shutdownNow();
            } catch (Exception e) {
                getLog().error("Problems shutting down!!!", e);
            }
        }
    }

    void setupAsyncQueue(Consumer<Collection<Append>> preSubmit, Consumer<Collection<Append>> postSubmit) {

        Callable<String> callable = () -> {
            try {
                while (!Thread.interrupted() && (!asyncFinished.get() || !this.submitQueue.isEmpty())) { // until interrupted
                    // Avoid deadlock if something unexpected happend with long wait.
                    Collection<Append> appends = this.submitQueue.poll(1, TimeUnit.HOURS);
                    if (null == appends) {
                        continue; // retry and finish, if flag is set.
                    }
                    if (Thread.interrupted()) {
                        break; //exit when interrupted
                    }
                    getLog().info("Received {} appends ", appends.size());
                    preSubmit.accept(appends);
                    doSubmit(appends);
                    getLog().info("Finished submitting {} appends ", appends.size());
                    postSubmit.accept(appends);
                }
                return "Done";
            } catch (Exception e) {
                getLog().error("Problems with Async processor", e);
                throw e;
            }
        };

        this.submitterPool = Executors.newFixedThreadPool(1,  (r) -> {
            Thread t = Executors.defaultThreadFactory().newThread(r);
            t.setDaemon(true);
            t.setName("HbaseTableMapper-submit-thread");
            return t;
        });
        submitFuture = this.submitterPool.submit(callable);
    }

    public void setSampleNameToSampleId(Map<String, Integer> sampleNameToSampleId) {
        this.sampleNameToSampleId = new HashMap<>(sampleNameToSampleId);
    }

    public Map<String, Integer> getSampleNameToSampleId() {
        return sampleNameToSampleId;
    }

    public void doSubmit(Collection<Append> appends) throws IOException {
        if (appends.isEmpty()) {
            return; // don't bother.
        }
        getHelper().getHBaseManager().act(getHelper().getOutputTableAsString(), table -> {
            for (Append append : appends) {
                if (null != append) {
                    table.append(append);
                }
            }
        });
    }

    public void submit(Collection<Append> appends) throws IOException {
        if (this.asyncPut.get()) {
            try {
                // blocking operation -> wait for 5 minutes, otherwise throw exception (avoid livelock)
                boolean isSubmitted = this.submitQueue.offer(appends, 10, TimeUnit.MINUTES);
                if (!isSubmitted) {
                    throw new IllegalStateException(
                            "Unable to add APPEND to queue - current size: " + this.submitQueue.size()
                                    + ": Future isDone: " + this.submitFuture.isDone()
                                    + ": Pool status: " + this.submitterPool);
                }
            } catch (InterruptedException e) {
                throw new IOException(e);
            }
        } else {
            doSubmit(appends);
        }
    }

    protected Map<Integer, Set<Integer>> convertListToSet(Map<Integer, List<Integer>> integerSetMap) {
        Map<Integer, Set<Integer>> map = new HashMap<>();
        integerSetMap.forEach((k, v) -> map.put(k, new HashSet<>(v)));
        return map;
    }

    public void printStats(HBaseAlleleCalculator calc) {
        getLog().info("Calc stats ... ");
        AtomicLong refCnt = new AtomicLong(0);
        AtomicLong altCnt = new AtomicLong(0);
        AtomicLong notCnt = new AtomicLong(0);
        AtomicLong passCnt = new AtomicLong(0);

        Consumer<AlleleCountPosition> action = pos -> {
            pos.getReference().forEach((a, v) -> refCnt.addAndGet(v.size()));
            pos.getAlternate().forEach((a, v) -> altCnt.addAndGet(v.size()));
            pos.getAltMap().forEach((a, m) -> m.forEach((b, v) -> altCnt.addAndGet(v.size())));
            notCnt.addAndGet(new HashSet<>(pos.getNotPass()).size());
            passCnt.addAndGet(pos.getPass().size());
        };
        calc.forEachPosition((pos, count) -> action.accept(count));
//        calc.forEachVariantPosition(pos -> calc.forEachVariant(pos, (var, count) -> action.accept(count)));

        long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
        usedMemory /= 1024 * 1024;
        getLog().info("REF:\t" + refCnt.get());
        getLog().info("ALT:\t" + altCnt.get());
        getLog().info("NOP:\t" + notCnt.get());
        getLog().info("PAS:\t" + passCnt.get());
        getLog().info("Found IDs:\t" + (refCnt.get() + altCnt.get() + notCnt.get() + passCnt.get()) + "\tUsed Memory:\t" + usedMemory);
    }

    void processCells(Collection<Cell> cells, Consumer<Variant> variantConsumer) {
        String studyId = getStudyConfiguration().getStudyId() + "";
        int i = 0;
        for (Cell cell : cells) {
            List<Variant> notResolved = getResultConverter().convert(cell, false);
            for (Variant variant : notResolved) { // fix that the read length is not set properly
                if (fixEndPosition) {
                    if (variant.getStudy(studyId).getFiles().get(0).getAttributes().containsKey(END)) {
                        variant.setEnd(variant.getEnd() - 1);
                        variant.getStudy(studyId).getFiles().get(0).getAttributes().put(END, variant.getEnd().toString());
                    }
                }
                if (variant.getEnd() > variant.getStart() && variant.getLength() == 1) {
                    variant.setLength((variant.getEnd() - variant.getStart()) + 1);
                }
            }
            List<Variant> converted = getResultConverter().resolveConflicts(notResolved);
            for (Variant variant : converted) {
                completeAlternateCoordinates(variant);
                variantConsumer.accept(variant);
            }
            if (++i % 1000 == 0) {
                long usedMemory = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
                usedMemory /= 1024 * 1024;
                getLog().info("Cell:\t" + i + "\tUsed Memory:\t" + usedMemory);
            }
        }
    }

    @Override
    protected void doMap(VariantMapReduceContext ctx) throws IOException, InterruptedException {
        try {
            int startPos = (int) ctx.getStartPos();
            int nextStartPos = (int) ctx.getNextStartPos();

            String studyId = Integer.valueOf(getStudyConfiguration().getStudyId()).toString();
            AlleleRegionCalculator alleleCalculator = new AlleleRegionCalculator(studyId, this.sampleNameToSampleId, startPos, nextStartPos - 1);
//                    new HBaseAlleleCalculator(studyId, this.sampleNameToSampleId, startPos, nextStartPos - 1);

            getLog().info("Read Archive ...");
            AtomicLong countVariants = new AtomicLong(0);
            List<Cell> cells = Arrays.stream(ctx.getValue().rawCells())
                    .filter(c -> Bytes.equals(CellUtil.cloneFamily(c), getHelper().getColumnFamily()))
                    .filter(c -> !Bytes.startsWith(CellUtil.cloneQualifier(c), GenomeHelper.VARIANT_COLUMN_B_PREFIX))
                    .collect(Collectors.toList());

            processCells(cells, variant -> {
                int from = toPosition(variant, true);
                int to = toPosition(variant, false);
                if (from <= nextStartPos && to >= startPos) {
                    alleleCalculator.addVariant(variant);
                    countVariants.incrementAndGet();
                }
            });
            getLog().info("Fill no-calls ... ");
            alleleCalculator.fillNoCalls(this.currentIndexingSamples, ctx.getStartPos(), ctx.getNextStartPos());
            getLog().info("Calculate sparse ... ");
            alleleCalculator.onlyLeaveSparseRepresentation(startPos, nextStartPos, false, false);
//            printStats(alleleCalculator);

            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VARIANTS_FROM_ARCHIVE").increment(countVariants.get());

            Collection<Append> appends = packageAlleleCounts(ctx.getChromosome(), studyId, alleleCalculator);

            /* Submit */
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "append-created").increment(appends.size());
            getLog().info("Submit {} appends ... ", appends.size());
            submit(appends);
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "append-submitted").increment(appends.size());
            ctx.getContext().getCounter(COUNTER_GROUP_NAME, "VCF_BLOCK_DONE").increment(1);
        } catch (Exception e) {
            // in case of Exception -> force shutdown!!!
            if (this.asyncPut.get()) {
                this.submitFuture.cancel(true);
            }
            throw e;
        }
    }

    protected Collection<Append> packageAlleleCounts(String chromosome, String studyId, AlleleRegionCalculator alleleCalculator) {
        return converter.convert(chromosome, alleleCalculator.getStore());
    }
}
