/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vilmaa.genome.storage.hbase.allele.transfer;

import vilmaa.genome.storage.hbase.allele.exporter.AlleleTableToVariantRunner;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.RawComparator;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.security.Credentials;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
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

            HBaseManager.HBaseTableConsumer consumer =
                    c -> mapper.myRun(c.getScanner(scan), put -> putList.add(put));

            getHelper().getHBaseManager().act(variantTable, consumer);
            prepareVcf(() -> {
                putList.forEach(p -> writeVcf(Result.create(p.getFamilyCellMap().get(getHelper().getColumnFamily()))));
            });
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

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
                return this.configuration;
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
