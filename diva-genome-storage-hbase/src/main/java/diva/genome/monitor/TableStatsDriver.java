package diva.genome.monitor;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;

/**
 * Created by mh719 on 03/03/2017.
 */
public class TableStatsDriver extends Configured implements Tool {


    public static class TableStatsMapper extends TableMapper<ImmutableBytesWritable, Result> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            value.listCells().forEach((cell) -> {
                context.getCounter("diva", Bytes.toString(CellUtil.cloneQualifier(cell))).increment(1);
            });
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        String table = args[0];
        Job job = Job.getInstance(getConf(), "TableStats");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        Scan scan = new Scan();
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        scan.addFamily(Bytes.toBytes(GenomeHelper.DEFAULT_COLUMN_FAMILY)); // Ignore PHOENIX columns!!!

        TableMapReduceUtil.initTableMapperJob(
                table,      // input table
                scan,             // Scan instance to control CF and attribute selection
                TableStatsMapper.class,   // mapper class
                ImmutableBytesWritable.class,             // mapper output key
                Result.class,             // mapper output value
                job,
                true);
        job.setOutputFormatClass(NullOutputFormatormat.class);
        job.setNumReduceTasks(0);

        boolean succeed = job.waitForCompletion(true);
        System.err.println("Job finished with " + succeed);
        return succeed ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        TableStatsDriver tool = new TableStatsDriver();
        tool.setConf(conf);
        int ret = ToolRunner.run(tool, args);
        if (ret != 0) {
            System.err.println("WARNING: Exit with value " + ret + "!!!!!!");
        }
        System.exit(ret);
    }
}
