package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import diva.genome.storage.hbase.allele.AbstractAlleleDriver;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.Objects;

/**
 * Created by mh719 on 27/02/2017.
 */
public class NonsenseDriver extends AbstractAlleleDriver {
    public static final String CONFIG_ANALYSIS_EXPORT_AVRO_PATH = "diva.genome.analysis.models.avro.file";


    private Path outAvroFile;

    public NonsenseDriver() { /* nothing */ }

    public NonsenseDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        super.parseAndValidateParameters();
        String file = null;
        if (!Objects.isNull(getConf().get(CONFIG_ANALYSIS_EXPORT_AVRO_PATH, null))) {
            file = getConf().get(CONFIG_ANALYSIS_EXPORT_AVRO_PATH);
        }
        if (StringUtils.isBlank(file)) {
            throw new IllegalArgumentException("No avro output path specified using " + CONFIG_ANALYSIS_EXPORT_AVRO_PATH);
        }
        outAvroFile = new Path(file);
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return GeneSummaryMapper.class;
    }

    private Class<? extends Reducer> getCombinerClass() {
        return GeneSummaryCombiner.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String analysisTable = getHelper().getOutputTableAsString();
        getLog().info("Read from {} table ...", analysisTable);
        super.initMapReduceJob(this.getCountTable(), job, scan, addDependencyJar);

        job.setCombinerClass(getCombinerClass());

        FileOutputFormat.setOutputPath(job, this.outAvroFile); // set Path
        FileOutputFormat.setOutputCompressorClass(job, SnappyCodec.class); // compression
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, GeneSummary.getClassSchema()); // Set schema
//        job.setNumReduceTasks(0);

        getLog().info("Write to {} ouptut file ...", analysisTable);

//        TableMapReduceUtil.initTableReducerJob(
//                analysisTable,      // output table
//                null,             // reducer class
//                job,
//                null, null, null, null,
//                addDependencyJar);
//        job.setNumReduceTasks(0);
    }


    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new NonsenseDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}