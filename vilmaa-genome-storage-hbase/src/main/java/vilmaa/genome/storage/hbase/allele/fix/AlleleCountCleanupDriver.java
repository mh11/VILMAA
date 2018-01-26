package vilmaa.genome.storage.hbase.allele.fix;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.KeyOnlyFilter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.NullOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

/**
 * Created by mh719 on 18/01/2018.
 */
public class AlleleCountCleanupDriver extends Configured implements Tool {

    private static final String BED_VALID_FILE = "vilmaa.fix.duster.bed.valid";
    public static final String DO_DELETE = "DO_DELETE";

    public static class Duster extends TableMapper<ImmutableBytesWritable, Mutation> {
        private final Logger log = LoggerFactory.getLogger(Duster.class);
        private volatile Map<String, NavigableMap<Integer, Integer>> regions = new HashMap<>();
        private volatile GenomeHelper helper = null;
        private byte[] studiesRow;
        private Boolean doDelete = false;

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            super.setup(context);
            doDelete = context.getConfiguration().getBoolean(DO_DELETE, false);
            log.info("Set delete to: " + doDelete);
            helper = new GenomeHelper(context.getConfiguration());
            studiesRow = helper.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
            File bedFile = new File(context.getConfiguration().get(BED_VALID_FILE,null));
            try(BufferedReader br = new BufferedReader(new FileReader(bedFile))) {
                String line = null;
                while (!Objects.isNull(line = br.readLine())) {
                    line = StringUtils.strip(line);
                    String[] arr = line.split("\t");
                    if (!line.isEmpty() && arr.length == 3) {
                        String chrom = arr[0];
                        Integer start = Integer.valueOf(arr[1]);
                        Integer end = Integer.valueOf(arr[2]);
                        this.regions.computeIfAbsent(chrom, s -> new TreeMap<>()).put(start, end);
                    }
                }
            }
            int sum = this.regions.values().stream().mapToInt(t -> t.size()).sum();
            log.info("Loaded " + sum + " regions ...");
        }

        private void deleteEntry(ImmutableBytesWritable key, Context context) throws IOException, InterruptedException {
            context.getCounter("VILMAA_DELETE",doDelete.toString()).increment(1);
            if (!doDelete) {
                return; // ignore - don't delete
            }
            ImmutableBytesWritable keyW = new ImmutableBytesWritable(key.get());
            Delete delete = new Delete(keyW.get());
            context.write(keyW, delete);
        }

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
            String vilmaaPOS = "VILMAA_POS";
            String vilmaaVAR = "VILMAA_VAR";
            if (Bytes.startsWith(key.get(), studiesRow)) {
                context.getCounter(vilmaaPOS, "META_ROW").increment(1);
                return;
            }
            Variant variant = helper.extractVariantFromVariantRowKey(key.get());
            String reference = Objects.isNull(variant.getReference()) ? "" : variant.getReference();
            String alternate = Objects.isNull(variant.getAlternate()) ? "" : variant.getAlternate();
            String vilmaaGroup = reference.isEmpty() && alternate.isEmpty() ? vilmaaPOS : vilmaaVAR;

            NavigableMap<Integer, Integer> positions = regions.get(variant.getChromosome());
            if (Objects.isNull(positions)) {
                context.getCounter(vilmaaGroup, "WRONG_CHROM").increment(1);
                return;
            }
            Integer start = variant.getStart();
            Integer entryKey = positions.floorKey(start);
            if (Objects.isNull(entryKey)) {
                context.getCounter(vilmaaGroup, "REGION_KEY_NULL").increment(1);
                deleteEntry(key, context);
                return;
            }
            Integer entryValue = positions.get(entryKey);
            if (entryValue < start) {
                context.getCounter(vilmaaGroup, "OUT_OF_REGION").increment(1);
                deleteEntry(key, context);
                return;
            }
            context.getCounter(vilmaaGroup, "VALID_POSITION").increment(1);
        }
    }

    @Override
    public int run(String[] strings) throws Exception {
        String tableName = strings[0];
        String bedPath = strings[1];
        Boolean doDelete = false;
        if (strings.length > 2) {
            doDelete = Boolean.valueOf(strings[2]);
        }
        File bedFile = new File(bedPath);
        // check File
        if (!bedFile.exists()) throw new IOException("File doesn't exist: " + bedFile);
        if (!bedFile.isFile()) throw new IOException("Not a file: " + bedFile);
        if (!bedFile.canRead()) throw new IOException("File not readable: " + bedFile);
        if (bedFile.length() < 1) throw new IOException("File is empty: " + bedFile);
        // set config
        getConf().set(BED_VALID_FILE, bedFile.getAbsolutePath());
        getConf().setBoolean(DO_DELETE, doDelete);
        // create Job
        Job job = Job.getInstance(getConf(), "Duster - clear up mess");
        job.getConfiguration().set("mapreduce.job.user.classpath.first", "true");
        job.setJarByClass(Duster.class);
        // create Scan
        Scan scan = new Scan();
        scan.setCacheBlocks(false);
        scan.addFamily(Bytes.toBytes(GenomeHelper.DEFAULT_COLUMN_FAMILY));
        scan.setFilter(new KeyOnlyFilter()); // only Keys, NOT values.
        // init job
        TableMapReduceUtil.initTableMapperJob(
                Bytes.toBytes(tableName), //input Table
                scan,
                Duster.class,
                null,
                null,
                job,
                true
        );
        if (doDelete) {
            TableMapReduceUtil.initTableReducerJob(tableName, null, job);
        } else {
            job.setOutputFormatClass(NullOutputFormat.class);
        }
        job.setNumReduceTasks(0);

        Thread hook = new Thread(() -> {
            try {
                if (!job.isComplete()) {
                    job.killJob();
                }
//                onError();
            } catch (IOException e) {
                e.printStackTrace();
                System.err.println("Error");
            }
        });
        Runtime.getRuntime().addShutdownHook(hook);
        boolean succeed = job.waitForCompletion(true);
        Runtime.getRuntime().removeShutdownHook(hook);
        return succeed ? 0 : 1;
    }

    public static void main(String[] args) {
        if (args.length != 2) {
            throw new IllegalArgumentException("Expected arguments: <table_name> <valid_regions.bed>");
        }
        try {
            AlleleCountCleanupDriver driver = new AlleleCountCleanupDriver();
            driver.setConf(HBaseConfiguration.create());
            System.exit(ToolRunner.run(driver, args));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
