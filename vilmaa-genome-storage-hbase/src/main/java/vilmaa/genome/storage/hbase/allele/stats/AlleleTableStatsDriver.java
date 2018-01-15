package vilmaa.genome.storage.hbase.allele.stats;

import com.google.common.collect.BiMap;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.mapreduce.Job;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractAnalysisTableDriver;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

/**
 * Execute MapReduce job to (re-)calculate statistics for different cohorts.
 * Created by mh719 on 01/02/2017.
 */
public class AlleleTableStatsDriver extends AbstractAnalysisTableDriver {

    public static final String CONFIG_STORAGE_STATS_COHORTS = "vilmaa.genome.storage.hbase.allele.stats.cohorts";
    public static final String CONFIG_STORAGE_STATS_HWE = "vilmaa.genome.storage.hbase.allele.stats.hwe";
    public static final String CONFIG_STORAGE_STATS_CALC = "vilmaa.genome.storage.hbase.allele.stats.CALC";


    public AlleleTableStatsDriver() { /* nothing */ }

    public AlleleTableStatsDriver(Configuration conf) {
        super(conf);
    }

    @Override
    protected void parseAndValidateParameters() {
        // nothing to do
    }

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AlleleStatsMapper.class;
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);
        TableMapReduceUtil.initTableReducerJob(
                inTable,      // output table
                null,             // reducer class
                job,
                null, null, null, null,
                addDependencyJar);
        job.setNumReduceTasks(0);
    }

    @Override
    protected void preExecution(String variantTable) throws IOException, StorageEngineException {
        String defaultCohortName = StudyEntry.DEFAULT_COHORT;
        // TODO needs to be removed after integration

        int studyId = getHelper().getStudyId();
        long lock = getStudyConfigurationManager().lockStudy(studyId);
        StudyConfiguration sc = null;
        try {
            sc = loadStudyConfiguration();
            BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(sc);

            final Integer defaultCohortId;
            if (sc.getCohortIds().containsKey(defaultCohortName)) { //Check if "defaultCohort" exists
                defaultCohortId = sc.getCohortIds().get(defaultCohortName);
                if (sc.getCalculatedStats().contains(defaultCohortId)) { //Check if "defaultCohort"
                    // is calculated
                    //Check if the samples number are different
                    if (!indexedSamples.values().equals(sc.getCohorts().get(defaultCohortId))) {
                        sc.getCalculatedStats().remove(defaultCohortId);
                        sc.getInvalidStats().add(defaultCohortId);
                    }
                }
            } else {
                throw new IllegalStateException("No default cohort found!!!");
            }
            sc.getCohorts().put(defaultCohortId, indexedSamples.values());
            getStudyConfigurationManager().updateStudyConfiguration(sc, new QueryOptions());
        } finally {
            getStudyConfigurationManager().unLockStudy(studyId, lock);
        }
        // update PHOENIX definition with statistic columns
        VariantPhoenixHelper variantPhoenixHelper = new VariantPhoenixHelper(getHelper());
        AlleleTablePhoenixHelper allelePhoenixHelper = new AlleleTablePhoenixHelper(getHelper());
        try (Connection connection = variantPhoenixHelper.newJdbcConnection()) {
            variantPhoenixHelper.updateStatsColumns(connection, variantTable, sc);
            allelePhoenixHelper.updateStatsColumns(connection, variantTable, sc);
        } catch (SQLException | ClassNotFoundException e) {
            getLog().error("Problems updating PHOENIX table!!!", e);
            throw new IllegalStateException("Problems updating PHOENIX table", e);
        }
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        if (succeed) {
            int studyId = getHelper().getStudyId();
            long lock = getStudyConfigurationManager().lockStudy(studyId);
            try {
                StudyConfiguration sc = loadStudyConfiguration();
                sc.setCalculatedStats(sc.getCohortIds().values()); // update
                sc.setInvalidStats(Collections.emptySet());
                getStudyConfigurationManager().updateStudyConfiguration(sc, new QueryOptions());
            } finally {
                getStudyConfigurationManager().unLockStudy(studyId, lock);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AlleleTableStatsDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }

}
