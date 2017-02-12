package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.stats.VariantTypeSummaryMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;

import java.io.IOException;

/**
 * Created by mh719 on 12/02/2017.
 */
public class VariantTypeSummaryDriver extends AbstractAnalysisTableDriver {

    public VariantTypeSummaryDriver() { /* nothing */ }

    public VariantTypeSummaryDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {

    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return VariantTypeSummaryMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);

        job.setOutputFormatClass(NullOutputFormat.class);
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new VariantTypeSummaryDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
