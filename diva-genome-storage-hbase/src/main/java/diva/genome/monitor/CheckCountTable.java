package diva.genome.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;

/**
 * Check consistency of Count Table
 * Created by mh719 on 07/11/2017.
 */
public class CheckCountTable extends Configured implements Tool {

    public class CheckCountTableMapper extends TableMapper<ImmutableBytesWritable, Result> {
        private Logger logger = LoggerFactory.getLogger(this.getClass());

        private GenomeHelper genomeHelper;
        private String chromosome = null;
        private Integer position = null;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            genomeHelper = new GenomeHelper(context.getConfiguration());
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            Variant variant = genomeHelper.extractVariantFromVariantRowKey(key.get());
            if (!variant.getType().equals(VariantType.NO_VARIATION)) {
                context.getCounter("diva","variant").increment(1);
                return; // only consider REFERENCE positions
            }
            if (Objects.isNull(chromosome) || !Objects.equals(chromosome, variant.getChromosome())) {
                context.getCounter("diva",Objects.isNull(chromosome) ? "init" : "chr_change").increment(1);
                chromosome = variant.getChromosome();
                position = variant.getStart();
                return; // first entry or change of chromosome
            }
            int nextPos = position + 1;
            if (!variant.getStart().equals(nextPos)) {
                // differences
                logger.error("Previous position: " + chromosome + ":" + position + "; " +
                        "Current position: " + variant.getChromosome() + ":" + variant.getStart());
                context.getCounter("diva","issue").increment(1);
            }
            position = variant.getStart(); // update
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String table = args[0];
        Job job = Job.getInstance(getConf(), "CheckCountTable");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        Scan scan = new Scan();
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes(GenomeHelper.DEFAULT_COLUMN_FAMILY)); // Ignore PHOENIX columns!!!

        TableMapReduceUtil.initTableMapperJob(
                table,      // input table
                scan,             // Scan instance to control CF and attribute selection
                CheckCountTableMapper.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                Result.class,             // mapper output value
                job,
                true);
        job.setOutputFormatClass(NullOutputFormat.class);
        job.setNumReduceTasks(0);

        boolean succeed = job.waitForCompletion(true);
        System.err.println("Job finished with " + succeed);
        return succeed ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        CheckCountTable tool = new CheckCountTable();
        tool.setConf(conf);
        int ret = ToolRunner.run(tool, args);
        if (ret != 0) {
            System.err.println("WARNING: Exit with value " + ret + "!!!!!!");
        }
        System.exit(ret);
    }

}
