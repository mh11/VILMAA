package diva.genome.storage.hbase.allele;

import diva.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver;
import diva.genome.storage.hbase.allele.transfer.HbaseTransferAlleleMapper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.HBASE_SCAN_CACHING;

/**
 * Created by mh719 on 30/01/2017.
 */
public class AlleleTransferDriver extends AbstractAnalysisTableDriver {
    public static final String CONFIG_ANALYSIS_TABLE = "opencga.analysis.allele.table.name";
    private String analysisTable;

    public AlleleTransferDriver() { /* nothing */ }

    public AlleleTransferDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        analysisTable = getConf().get(CONFIG_ANALYSIS_TABLE, StringUtils.EMPTY);
        if (StringUtils.isBlank(analysisTable)) {
            throw new IllegalStateException("Analysis table paramter required: " + CONFIG_ANALYSIS_TABLE);
        }
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        getLog().info("Create table {} (if needed)", this.analysisTable);
        AlleleTablePhoenixHelper phoenixHelper = new AlleleTablePhoenixHelper(getHelper());
        try (Connection jdbcCon = phoenixHelper.getHelper().newJdbcConnection()) {
            AbstractVariantTableDriver.createVariantTableIfNeeded(getHelper(), analysisTable);
            phoenixHelper.registerNewStudy(jdbcCon, this.analysisTable);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        } catch (SQLException e) {
            throw new IllegalStateException(e);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException(e);
        }

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
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        getLog().info("Write output to {} table ...", this.analysisTable);
        TableMapReduceUtil.initTableReducerJob(
                this.analysisTable,      // output table
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
