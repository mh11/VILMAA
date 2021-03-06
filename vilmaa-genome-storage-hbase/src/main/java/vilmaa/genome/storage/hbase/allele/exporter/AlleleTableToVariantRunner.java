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

package vilmaa.genome.storage.hbase.allele.exporter;

import com.google.common.collect.BiMap;
import vilmaa.genome.storage.hbase.allele.AbstractLocalRunner;
import vilmaa.genome.storage.hbase.allele.count.AlleleCountPosition;
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToVariantConverter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.io.RawComparator;
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
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URI;
import java.util.Collections;


/**
 * Created by mh719 on 08/02/2017.
 */
public class AlleleTableToVariantRunner extends AbstractLocalRunner {
    public static final String OUTPUT_VCF_FILE = "vilmaa.allele.output.vcf";
    private HBaseAlleleCountsToVariantConverter variantConverter;
    private VariantVcfDataWriter vcfDataWriter;
    private MyMapper myMapper;

    @Override
    protected void map(Scan scan, String variantTable) {
        try {
            myMapper = new MyMapper();
            MyMapper.MyCtxt ctxt = myMapper.buildContext();
            ctxt.configuration = getConf();
            myMapper.setup(ctxt);
            prepareVcf(() -> super.map(scan, variantTable));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (InterruptedException e) {
            throw new IllegalStateException(e);
        }
    }

    protected void prepareVcf(Runnable runnable) throws IOException {
        File outVCF = getOutputFile();
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(getStudyConfiguration());
        variantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        variantConverter.setReturnSamples(indexedSamples.keySet());
        variantConverter.setStudyNameAsStudyId(true);
        QueryOptions options = new QueryOptions();
        VariantSourceDBAdaptor source = new HadoopVariantSourceDBAdaptor(getHelper());

        try (OutputStream out = new FileOutputStream(outVCF)) {
            HadoopVcfVilmaaOutputFormat outputFormat = new HadoopVcfVilmaaOutputFormat();
            vcfDataWriter = outputFormat.prepareVcfWriter(
                    getHelper(), getStudyConfiguration(), (a, b) -> {}, out);
            vcfDataWriter.open();
            vcfDataWriter.pre();
            // do the work
            runnable.run();
            // clean up
            vcfDataWriter.post();
            vcfDataWriter.close();
        } catch (Exception e) {
            getLog().error("Problems with VCF conversion", e);
            throw new IllegalStateException(e);
        }
    }

    private File getOutputFile() {
        String outVcf = getConf().get(OUTPUT_VCF_FILE, StringUtils.EMPTY);
        if (StringUtils.isBlank(outVcf)) {
            throw new IllegalStateException("VCF output paramter required: " + OUTPUT_VCF_FILE);
        }
        File outFile = new File(outVcf);
        if (outFile.exists()) {
            throw new IllegalStateException("VCF output already exists !!!");
        }
        if (!outFile.getParentFile().exists()) {
            throw new IllegalStateException("VCF output directory does not exist !!!" + outFile.getParentFile());
        }
        return outFile;
    }

    @Override
    protected void map(Result result) throws IOException {
        try {
            myMapper.doMap(result);
            Variant currVar = (Variant) myMapper.buildContext().currWriteKey;
            if (null != currVar) {
                writeVcf(currVar);
            }
        } catch (InterruptedException e) {
            throw new IOException(e);
        }
    }

    protected void writeVcf(Result result) {
        Variant variant = variantConverter.convert(result);
        if (getLog().isDebugEnabled()) {
            AlleleCountPosition convert = variantConverter.getAlleleCountConverter().convert(result);
            getLog().debug("Convert {} from \n{} ", variant, convert.toDebugString());
        }
        vcfDataWriter.write(Collections.singletonList(variant));
    }

    protected void writeVcf(Variant variant) {
        if (getLog().isDebugEnabled()) {
            getLog().debug("Convert {} ", variant);
        }
        vcfDataWriter.write(Collections.singletonList(variant));
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new AlleleTableToVariantRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

    private static class MyMapper extends AlleleTableToVariantMapper {
        private final MyCtxt ctxt;

        public MyMapper() {
            this.ctxt = new MyCtxt();
        }

        public MyCtxt buildContext() {
            return ctxt;
        }

        public void doMap(Result value) throws IOException, InterruptedException {
            ctxt.currWriteKey = null; // reset
            ctxt.currWriteValue = null; // reset
            this.map(null, value, ctxt);
        }

        public class MyCtxt extends Context {

            public Configuration configuration;
            public Object currWriteKey;
            public Object currWriteValue;

            @Override
            public InputSplit getInputSplit() {
                return null;
            }

            @Override
            public boolean nextKeyValue() throws IOException, InterruptedException {
                return false;
            }

            @Override
            public ImmutableBytesWritable getCurrentKey() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public Result getCurrentValue() throws IOException, InterruptedException {
                return null;
            }

            @Override
            public void write(Object o, Object o2) throws IOException, InterruptedException {
                this.currWriteKey = o;
                this.currWriteValue = o2;
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
                return new Counters.Counter();
            }

            @Override
            public Counter getCounter(String s, String s1) {
                return new Counters.Counter();
            }

            @Override
            public Configuration getConfiguration() {
                return configuration;
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
