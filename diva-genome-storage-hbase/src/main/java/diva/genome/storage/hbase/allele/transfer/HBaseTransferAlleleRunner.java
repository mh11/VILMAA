package diva.genome.storage.hbase.allele.transfer;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
import diva.genome.storage.hbase.allele.count.AlleleCountToHBaseConverter;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import diva.genome.storage.hbase.allele.exporter.AlleleTableToVariantRunner;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobID;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskAttemptID;
import org.apache.hadoop.mapreduce.TaskID;
import org.apache.hadoop.security.Credentials;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.OpencgaMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;


/**
 * Created by mh719 on 01/02/2017.
 */
public class HBaseTransferAlleleRunner extends AlleleTableToVariantRunner {

    @Override
    protected void map(Scan scan, String variantTable) {

        List<Put> putList = new ArrayList<>();
        try {
            MyMapper mapper = new MyMapper();
            MyMapper.MyCtxt myCtxt = mapper.buildContext();
            myCtxt.configuration = getConf();
            mapper.setup(myCtxt);

            HBaseManager.HBaseTableConsumer consumer =
                    c -> mapper.myRun(c.getScanner(scan), put -> putList.add(put));

            getHelper().getHBaseManager().act(variantTable, consumer);
            prepareVcf(() -> {
                putList.forEach(p -> writeVcf(Result.create(p.getFamilyCellMap().get(getHelper().getColumnFamily()))));
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            new IllegalStateException(e);
        }
    }

//    protected MyMapper buildMapper(VariantTableHelper gh, StudyConfiguration studyConfiguration) throws IOException {
//        MyMapper mapper = new MyMapper();
//        OpencgaMapReduceHelper mrHelper = new OpencgaMapReduceHelper(null);
//        mrHelper.setHelper(gh);
//        mrHelper.setStudyConfiguration(studyConfiguration);
//        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
//        mrHelper.setIndexedSamples(indexedSamples);
//        mrHelper.setTimestamp(1);
//
//        mapper.setMrHelper(mrHelper);
//        mapper.alleleCountConverter = new HBaseToAlleleCountConverter();
//        mapper.setStudiesRow(gh.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0));
//        mapper.alleleCombiner = new AlleleCombiner(new HashSet<>(indexedSamples.values()));
//        mapper.converter = new AlleleCountToHBaseConverter(gh.getColumnFamily(), gh.getStudyId() + "");
//        return mapper;
//    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new HBaseTransferAlleleRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static class MyMapper extends HbaseTransferAlleleMapper {
        private final MyCtxt ctxt;

        public MyMapper() {
            this.ctxt = new MyCtxt();
        }

        public MyCtxt buildContext() {
            return ctxt;
        }

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            // nothing
        }

        public void myRun(ResultScanner scanner, Consumer<Put> consumer) {
            ctxt.results = scanner.iterator();
            ctxt.resconsumer = consumer;
            try {
                this.run(ctxt);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            } catch (InterruptedException e) {
                throw new IllegalStateException(e);
            }
        }

//        @Override
//        protected Put newTransfer(Variant variant, AlleleCountPosition from, AlleleCountPosition to) {
//            Put put = super.newTransfer(variant, from, to);
//            if (getLog().isDebugEnabled()) {
//                getLog().debug("Merged from: \n{}" , from.toDebugString());
//                getLog().debug("Merged {} into: \n{}" , variant, to.toDebugString());
//            }
//            return put;
//        }

//        public void runAlternative(ResultScanner scanner, Consumer<Put> submitFunction) {
//            try {
//                // buffer
//                String chromosome = "-1";
//                Integer referencePosition = -1;
//                Result referenceResult = null;
//                AlleleCountPosition refBean = null;
//                clearRegionOverlap();
//                for (Result result : scanner) {
//                    if (isMetaRow(result.getRow())) {
//                        continue;
//                    }
//                    Variant variant = getHelper().extractVariantFromVariantRowKey(result.getRow());
//                    if (variant.getStart() > referencePosition && !positionBuffer.isEmpty()) {
//                        getLog().info("Process buffer of {} for position ... ", positionBuffer.size(), referencePosition);
//
//                        processBuffer(refBean, submitFunction);
//                        positionBuffer.clear();
//                    }
//                    if (!StringUtils.equals(chromosome, variant.getChromosome())) {
//                        referencePosition = -1;
//                        clearRegionOverlap();
//                        chromosome = variant.getChromosome();
//                    }
//                    checkDeletionOverlapMap(variant.getStart());
//                    if (variant.getType().equals(VariantType.NO_VARIATION)) {
//                        referencePosition = variant.getStart();
//                        referenceResult = result;
//                        refBean = null;
//                        continue;
//                    }
//                    // if actual variant
//                    if (null == referencePosition || !referencePosition.equals(variant.getStart())) {
//                        // should only happen at the start of a split block.
//                        referenceResult = queryForRefernce(variant);
//                    }
//                    if (refBean == null) {
//                        refBean = this.alleleCountConverter.convert(referenceResult);
//                    }
//                    this.positionBuffer.add(new ImmutablePair<>(result, variant));
//                }
//                getLog().info("Clear buffer ...");
//                if (!positionBuffer.isEmpty()) {
//                    processBuffer(refBean, submitFunction);
//                    positionBuffer.clear();
//                }
//                getLog().info("Done ...");
//            } catch (Exception e) {
//                throw new IllegalStateException("Something went wrong during transfer", e);
//            }
//        }

        private class MyCtxt extends Context {
            public Configuration configuration;
            public Iterator<Result> results;
            public Result currResult;
            public Consumer<Put> resconsumer;

            @Override
            public InputSplit getInputSplit() {
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                currResult = null;
                if (results.hasNext()) {
                    currResult = results.next();
                    return true;
                }
                return false;
            }

            @Override
            public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
                return new ImmutableBytesWritable(currResult.getRow());
            }

            @Override
            public Result getCurrentValue() throws IOException, InterruptedException {
                return currResult;
            }

            @Override
            public void write(ImmutableBytesWritable immutableBytesWritable, Mutation mutation) throws IOException,
                    InterruptedException {
                resconsumer.accept((Put) mutation);
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return new TaskAttemptID(new TaskID(new JobID("123", 1), TaskType.MAP, 1), 1);
            }

            @Override
            public void setStatus(String s) {

            }

            @Override
            public String getStatus() {
                return null;
            }

            @Override
            public float getProgress() {
                return 0;
            }

            @Override
            public Counter getCounter(Enum<?> anEnum) {
                return new org.apache.hadoop.mapred.Counters.Counter();
            }

            @Override
            public Counter getCounter(String s, String s1) {
                return new Counters.Counter();
            }

            @Override
            public Configuration getConfiguration() {
                return null;
            }

            @Override
            public Credentials getCredentials() {
                return null;
            }

            @Override
            public JobID getJobID() {
                return null;
            }

            @Override
            public int getNumReduceTasks() {
                return 0;
            }

            @Override
            public Path getWorkingDirectory() throws IOException {
                return null;
            }

            @Override
            public Class<?> getOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getOutputValueClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputKeyClass() {
                return null;
            }

            @Override
            public Class<?> getMapOutputValueClass() {
                return null;
            }

            @Override
            public String getJobName() {
                return null;
            }

            @Override
            public Class<? extends InputFormat<?, ?>> getInputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Mapper<?, ?, ?, ?>> getMapperClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getCombinerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Reducer<?, ?, ?, ?>> getReducerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends OutputFormat<?, ?>> getOutputFormatClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public Class<? extends Partitioner<?, ?>> getPartitionerClass() throws ClassNotFoundException {
                return null;
            }

            @Override
            public RawComparator<?> getSortComparator() {
                return null;
            }

            @Override
            public String getJar() {
                return null;
            }

            @Override
            public RawComparator<?> getCombinerKeyGroupingComparator() {
                return null;
            }

            @Override
            public RawComparator<?> getGroupingComparator() {
                return null;
            }

            @Override
            public boolean getJobSetupCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getTaskCleanupNeeded() {
                return false;
            }

            @Override
            public boolean getProfileEnabled() {
                return false;
            }

            @Override
            public String getProfileParams() {
                return null;
            }

            @Override
            public Configuration.IntegerRanges getProfileTaskRange(boolean b) {
                return null;
            }

            @Override
            public String getUser() {
                return null;
            }

            @Override
            public boolean getSymlink() {
                return false;
            }

            @Override
            public Path[] getArchiveClassPaths() {
                return new Path[0];
            }

            @Override
            public URI[] getCacheArchives() throws IOException {
                return new URI[0];
            }

            @Override
            public URI[] getCacheFiles() throws IOException {
                return new URI[0];
            }

            @Override
            public Path[] getLocalCacheArchives() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getLocalCacheFiles() throws IOException {
                return new Path[0];
            }

            @Override
            public Path[] getFileClassPaths() {
                return new Path[0];
            }

            @Override
            public String[] getArchiveTimestamps() {
                return new String[0];
            }

            @Override
            public String[] getFileTimestamps() {
                return new String[0];
            }

            @Override
            public int getMaxMapAttempts() {
                return 0;
            }

            @Override
            public int getMaxReduceAttempts() {
                return 0;
            }

            @Override
            public void progress() {

            }
        }

    }

}
