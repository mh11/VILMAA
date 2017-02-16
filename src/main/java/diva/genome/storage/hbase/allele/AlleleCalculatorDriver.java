package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.count.HbaseTableMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveHelper;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.AbstractAlleleDriver.CONFIG_COUNT_TABLE;

/**
 * Created by mh719 on 30/01/2017.
 */
public class AlleleCalculatorDriver extends AbstractVariantTableDriver {
    protected static final Logger LOG = LoggerFactory.getLogger(AlleleCalculatorDriver.class);
    private String countTable;
    public String getCountTable() {
        return countTable;
    }

    public static final String JOB_OPERATION_NAME = "Count Alleles";


    public AlleleCalculatorDriver() { /* nothing */ }

    private Logger getLog() {
        return LOG;
    }

    @Override
    public int run(String[] args) throws Exception {
        int fixedSizeArgs = 5;
        getConf().set(ArchiveDriver.CONFIG_ARCHIVE_TABLE_NAME, args[1]);
        getConf().set(CONFIG_VARIANT_TABLE_NAME, args[2]);
        getConf().set(GenomeHelper.CONFIG_STUDY_ID, args[3]);
        getConf().setStrings(CONFIG_VARIANT_FILE_IDS, args[4].split(","));
        for (int i = fixedSizeArgs; i < args.length; i = i + 2) {
            getConf().set(args[i], args[i + 1]);
        }

        // Set parallel pool size
        Collection<String> opts = getConf().getStringCollection("opencga.variant.table.mapreduce.map.java.opts");
        String optString = StringUtils.EMPTY;
        if (!opts.isEmpty()) {
            for (String opt : opts) {
                optString += opt + " ";
            }
        }
        if (StringUtils.isNotBlank(optString)) {
            getLog().info("Set mapreduce java opts: {}", optString);
            getConf().set("mapreduce.map.java.opts", optString);
        }
        countTable = getConf().get(CONFIG_COUNT_TABLE, StringUtils.EMPTY);
        if (StringUtils.isBlank(countTable)) {
            throw new IllegalStateException("Count table parameter required: " + CONFIG_COUNT_TABLE);
        }
        return super.run(args);
    }

    protected Scan createScan(VariantTableHelper gh, String[] fileArr) {
        Scan scan = new Scan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, 50);
        getLog().info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);  // don't set to true for MR jobs
        // https://hbase.apache.org/book.html#perf.hbase.client.seek
        int lookAhead = getConf().getInt("hbase.load.variant.scan.lookahead", -1);
        if (lookAhead > 0) {
            getLog().info("Scan set LOOKAHEAD to " + lookAhead);
            scan.setAttribute(Scan.HINT_LOOKAHEAD, Bytes.toBytes(lookAhead));
        }
        // specify return columns (file IDs)
        for (String fileIdStr : fileArr) {
            int id = Integer.parseInt(fileIdStr);
            byte[] column = Bytes.toBytes(ArchiveHelper.getColumnName(id));
            scan.addColumn(gh.getColumnFamily(), column);
        }
        return scan;
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return HbaseTableMapper.class;
    }

    @Override
    protected String getJobOperationName() {
        return JOB_OPERATION_NAME;
    }

    @Override
    protected void initMapReduceJob(String inTable, String outTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String countTable = getCountTable();
        getLog().info("Read from {} and write to {} ...", inTable, countTable);
        try (Connection con = ConnectionFactory.createConnection(getHelper().getConf())) {
            createHBaseTable(getHelper(), countTable, con); // NO PHOENIX needed!!!!
        }
        TableMapReduceUtil.initTableMapperJob(
                inTable,      // input table
                scan,             // Scan instance to control CF and attribute selection
                getMapperClass(),   // mapper class
                null,             // mapper output key
                null,             // mapper output value
                job,
                addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                countTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleCalculatorDriver()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, AlleleCalculatorDriver driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }


    public static String buildCommandLineArgs(String server, String archive, String countTable, String analysisTable, int studyId,
                                              List<Integer> fileIds, Map<String, Object> other) {
        StringBuilder stringBuilder = new StringBuilder().append(server).append(' ').append(archive).append(' ')
                .append(analysisTable).append(' ').append(studyId).append(' ');

        stringBuilder.append(fileIds.stream().map(Object::toString).collect(Collectors.joining(",")));
        stringBuilder.append(" ").append(CONFIG_COUNT_TABLE).append(" ").append(countTable);
        ArchiveDriver.addOtherParams(other, stringBuilder);
        return stringBuilder.toString();
    }

}
