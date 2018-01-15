package vilmaa.genome.storage.hbase.allele.stats;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.CONFIG_VARIANT_TABLE_NAME;

/**
 * Created by mh719 on 01/03/2017.
 */
public class AlleleTableStatsRunner extends AlleleTableStatsDriver {


    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleTableStatsRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    @Override
    protected boolean executeJob(Job job) throws IOException, InterruptedException, ClassNotFoundException {
        MyMapper myMapper = new MyMapper();
        MyMapper.MyContext context = myMapper.createContext(getConf());
        myMapper.setup(context);

        Scan scan = createScan();
        String variantTable = getConf().get(CONFIG_VARIANT_TABLE_NAME, StringUtils.EMPTY);
        VariantTableHelper helper = getHelper();
        try {
            helper.getHBaseManager().act(variantTable, c -> {
                try {
                    int iCnt = 0;
                    ResultScanner scanner = c.getScanner(scan);
                    for (Result result : scanner) {
                        if (iCnt % 1000 == 0) {
                            getLog().info("Processed {} variants and {} submitted ...", iCnt, context.submitted.size());
                            context.printCounts();
                        }
                        myMapper.map(new ImmutableBytesWritable(result.getRow()), result, context);
                        ++iCnt;
                    }
                } catch (InterruptedException e) {
                   throw new IllegalStateException(e);
                }
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
        return true;
    }



    public static class MyMapper extends AlleleStatsMapper {
        private Map<String, Counter> counterMap = new HashMap<>();

        public MyContext createContext(Configuration conf) {
            return new MyContext(conf);
        }

        public class MyContext extends Mapper.Context {
            private Configuration conf;
            public List<Pair<Object, Object>> submitted = new ArrayList<>();

            public MyContext(Configuration conf) {
                this.conf = conf;
            }

            public void printCounts() {
                counterMap.forEach((k, v) -> getLog().info("... {} -> {}", k, v.getValue()));
            }


            @Override
            public InputSplit getInputSplit() {
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public Object getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public Object getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public void write(Object o, Object o2) throws IOException, InterruptedException {
                submitted.add(new ImmutablePair<>(o, o2));
            }

            @Override
            public OutputCommitter getOutputCommitter() {
                return null;
            }

            @Override
            public TaskAttemptID getTaskAttemptID() {
                return new TaskAttemptID("",1, TaskType.MAP, 1, 1);
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
                return counterMap.computeIfAbsent(anEnum.toString(), (x) -> new org.apache.hadoop.mapred.Counters.Counter());
            }

            @Override
            public Counter getCounter(String s, String s1) {
                return counterMap.computeIfAbsent(s + "_" + s1, (x) -> new org.apache.hadoop.mapred.Counters.Counter());
            }

            @Override
            public Configuration getConfiguration() {
                return this.conf;
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
                return "TEST";
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
