package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.transfer.HbaseTransferAlleleMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;

import java.io.IOException;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.HBASE_SCAN_CACHING;

/**
 * Created by mh719 on 30/01/2017.
 */
public class AlleleTransferDriver extends AbstractAlleleDriver {

    public AlleleTransferDriver() { /* nothing */ }

    public AlleleTransferDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected Scan createScan() {
        Scan scan = super.createScan();
        int caching = getConf().getInt(HBASE_SCAN_CACHING, -1);
        if (caching > 1) {
            getLog().info("Scan set Caching to " + caching);
            scan.setCaching(caching);        // 1 is the default in Scan, 200 caused timeout issues.
        }
        return scan;
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return HbaseTransferAlleleMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        String analysisTable = getHelper().getOutputTableAsString();
        getLog().info("Read from {} table ...", this.getCountTable());
        getLog().info("Write to {} table ...", analysisTable);
        super.initMapReduceJob(this.getCountTable(), job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                analysisTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleTransferDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }


}
