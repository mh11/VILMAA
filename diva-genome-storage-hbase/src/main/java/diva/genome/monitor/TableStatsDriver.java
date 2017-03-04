package diva.genome.monitor;

import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
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
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.HashSet;

import static diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter.FILTER_PASS_CHAR;

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

    public static class TableStatsNoCallMapper extends TableMapper<ImmutableBytesWritable, Result> {
        public static final String NO_CALL = HBaseToAlleleCountConverter.REFERENCE_PREFIX_CHAR + "-1";
        public static final String FILTER_FAIL = FILTER_PASS_CHAR + "";
        private HashSet failed;
        private HashSet nocall;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            nocall = new HashSet<>();
            failed = new HashSet<>();
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            try {
                nocall.clear();
                failed.clear();
                for (Cell cell : value.listCells()) {
                    String qualifier = Bytes.toString(CellUtil.cloneQualifier(cell));
                    context.getCounter("diva", qualifier).increment(1);
                    switch (qualifier) {
                        case NO_CALL:
                            nocall.addAll(HBaseToAlleleCountConverter.getValueAsList(CellUtil.cloneValue(cell)));
                            break;
                        case FILTER_FAIL:
                            failed.addAll(HBaseToAlleleCountConverter.getValueAsList(CellUtil.cloneValue(cell)));
                            break;
                    }
                }
                if (!nocall.isEmpty() && !failed.isEmpty()) {
                    int nocallTotal = nocall.size();
                    nocall.retainAll(failed);
                    int nocallPass = nocallTotal - nocall.size();
                    context.getCounter("diva", "nocall-pass").increment(nocallPass);
                    context.getCounter("diva", "nocall-fail").increment(nocall.size());
                }
            } catch (Exception e) {
                throw new IllegalStateException(e);
            }
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
                TableStatsNoCallMapper.class,   // mapper class
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
        TableStatsDriver tool = new TableStatsDriver();
        tool.setConf(conf);
        int ret = ToolRunner.run(tool, args);
        if (ret != 0) {
            System.err.println("WARNING: Exit with value " + ret + "!!!!!!");
        }
        System.exit(ret);
    }
}
