package vilmaa.genome.storage.hbase.allele.fix;

import vilmaa.genome.storage.hbase.allele.AbstractAlleleDriver;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.opencb.opencga.storage.hadoop.utils.HBaseManager;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver
        .CONFIG_VARIANT_TABLE_COMPRESSION;
import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.HBASE_SCAN_CACHING;

/**
 * Created by mh719 on 09/02/2017.
 */
public class FromPhoenixToProto extends AbstractAlleleDriver {
    public static final String CONFIG_PROTO_FIX_TABLE = "vilmaa.genome.allele.countproto.table.name";
    protected static final Logger LOG = LoggerFactory.getLogger(FromPhoenixToProto.class);
    public static final String JOB_OPERATION_NAME = "Move Alleles";

    public FromPhoenixToProto() { /* nothing */ }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return FromPhoenixToProtoMapper.class;
//        return FromPhoenixToProto.SimpleCopy.class;
    }


    @Override
    protected Scan createScan() {
        Scan scan = super.createScan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, 100);
        getLog().info("Scan set Caching to " + caching);
        scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        scan.setCacheBlocks(false);
        return scan;
    }

    public boolean createHBaseTable(GenomeHelper genomeHelper, String tableName, Connection con) throws IOException {
        return HBaseManager.createTableIfNeeded(con, tableName, genomeHelper.getColumnFamily(), null,
                Compression.getCompressionAlgorithmByName(
                        genomeHelper.getConf().get(CONFIG_VARIANT_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName())));
    }

    @Override
    protected void checkTablesExist(GenomeHelper genomeHelper, String... tables) {
        String protoFixTable = getConf().get(CONFIG_PROTO_FIX_TABLE, "");
        if (StringUtils.isBlank(protoFixTable)) {
            throw new IllegalStateException("Please provide proto Fix table name using " + CONFIG_PROTO_FIX_TABLE);
        }
        getLog().info("Make sure Proto Count table exist ...", protoFixTable);
        try (Connection con = ConnectionFactory.createConnection(getHelper().getConf())) {
            this.createHBaseTable(getHelper(), protoFixTable, con); // NO PHOENIX!!!!
//            createVariantTableIfNeeded(getHelper(), protoFixTable, con); // With phoenix for comparison
        } catch (IOException e) {
            throw new IllegalStateException("Problems creating Table " + protoFixTable);
        }
        super.checkTablesExist(genomeHelper, tables);
    }

    @Override
    protected void initMapReduceJob(String variantTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String protoFixTable = getConf().get(CONFIG_PROTO_FIX_TABLE, "");
        getLog().info("Read from {} table ...", this.getCountTable());
        getLog().info("Write to {} table ...", protoFixTable);
        super.initMapReduceJob(this.getCountTable(), job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                protoFixTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new FromPhoenixToProto()));
        } catch (Exception e) {
            LOG.error("Problems", e);
            e.printStackTrace();
            System.exit(1);
        }
    }

    public static int privateMain(String[] args, Configuration conf, FromPhoenixToProto driver) throws Exception {
        // info https://code.google.com/p/temapred/wiki/HbaseWithJava
        if (conf == null) {
            conf = HBaseConfiguration.create();
        }
        driver.setConf(conf);
        int exitCode = ToolRunner.run(driver, args);
        return exitCode;
    }

    @Override
    public Logger getLog() {
        return this.LOG;
    }

    public static class SimpleCopy extends TableMapper<ImmutableBytesWritable, Mutation> {

        @Override
        protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
                InterruptedException {
            Put put = new Put(key.get());
            for (Cell cell : value.listCells()) {
                put.add(cell);
            }
            context.write(key, put);
        }
    }

}
