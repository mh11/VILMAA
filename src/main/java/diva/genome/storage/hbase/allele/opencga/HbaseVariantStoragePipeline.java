package diva.genome.storage.hbase.allele.opencga;

import diva.genome.storage.hbase.allele.AlleleCalculatorDriver;
import diva.genome.storage.hbase.allele.AlleleTransferDriver;
import diva.genome.storage.hbase.allele.AbstractAlleleDriver;
import diva.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.config.StorageConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.io.VariantReaderUtils;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.HadoopDirectVariantStoragePipeline;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.executors.MRExecutor;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

import static org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine.*;
import static org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableDriver.createHBaseTable;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HbaseVariantStoragePipeline extends HadoopDirectVariantStoragePipeline {

    protected final String countTableName;

    public HbaseVariantStoragePipeline(StorageConfiguration configuration, String storageEngineId, VariantHadoopDBAdaptor dbAdaptor, MRExecutor mrExecutor, Configuration conf, HBaseCredentials archiveCredentials, VariantReaderUtils variantReaderUtils, ObjectMap options) {
        super(configuration, storageEngineId, dbAdaptor, mrExecutor, conf, archiveCredentials, variantReaderUtils, options);
        this.countTableName = options.getString(AbstractAlleleDriver.CONFIG_COUNT_TABLE, "");
        if (StringUtils.isBlank(this.countTableName)) {
            throw new IllegalStateException("Count table required: " + AbstractAlleleDriver.CONFIG_COUNT_TABLE);
        }
    }

    @Override
    protected void preMerge(URI input) throws StorageEngineException {
        int studyId = getStudyId();
        // create count table
        try ( org.apache.hadoop.hbase.client.Connection con = ConnectionFactory.createConnection(dbAdaptor.getConfiguration())) {
            createHBaseTable(dbAdaptor.getGenomeHelper(), this.countTableName, con); // NO PHOENIX needed!!!!
        } catch (IOException e) {
            throw new StorageEngineException("Unable to create count table " + this.countTableName);
        }

        // create Analysis table
        AlleleTablePhoenixHelper phoenixHelper = new AlleleTablePhoenixHelper(dbAdaptor.getGenomeHelper());
        try {
            Connection jdbcConnection = dbAdaptor.getJdbcConnection();
            String tableName = getVariantTableName();
            phoenixHelper.registerNewStudy(jdbcConnection, tableName, studyId);
            if (!options.getBoolean(SKIP_CREATE_PHOENIX_INDEXES, false)) {
                if (options.getString(VariantAnnotationManager.SPECIES, "hsapiens").equalsIgnoreCase("hsapiens")) {
                    List<PhoenixHelper.Column> columns = VariantPhoenixHelper.getHumanPopulationFrequenciesColumns();
                    phoenixHelper.getPhoenixHelper().addMissingColumns(jdbcConnection, tableName, columns, true);
                    List<PhoenixHelper.Index> popFreqIndices = VariantPhoenixHelper.getPopFreqIndices(tableName);
                    phoenixHelper.getPhoenixHelper().createIndexes(jdbcConnection, tableName, popFreqIndices, false);
                }
                phoenixHelper.createVariantIndexes(jdbcConnection, tableName);
            } else {
                logger.info("Skip create indexes!!");
            }
        } catch (SQLException e) {
            throw new StorageEngineException("Unable to register study in Phoenix", e);
        }

        long lock = dbAdaptor.getStudyConfigurationManager().lockStudy(studyId);

        //Get the studyConfiguration. If there is no StudyConfiguration, create a empty one.
        try {
            StudyConfiguration studyConfiguration = checkOrCreateStudyConfiguration(true);
            VariantSource source = readVariantSource(input, options);
            securePreMerge(studyConfiguration, source);
            dbAdaptor.getStudyConfigurationManager().updateStudyConfiguration(studyConfiguration, null);
        } finally {
            dbAdaptor.getStudyConfigurationManager().unLockStudy(studyId, lock);
        }
    }

    @Override
    public void merge(int studyId, List<Integer> pendingFiles) throws StorageEngineException {
        // Check if status is "DONE"
        if (options.get(HADOOP_LOAD_VARIANT_STATUS, BatchFileOperation.Status.class).equals(BatchFileOperation.Status.DONE)) {
            // Merge operation status : DONE, not READY or RUNNING
            // Don't need to merge again. Skip merge and run post-load/post-merge step
            logger.info("Files {} already merged!", pendingFiles);
            return;
        }
        String hadoopRoute = options.getString(HADOOP_BIN, "hadoop");
        String jar = getJarWithDependencies();
        options.put(HADOOP_LOAD_VARIANT_PENDING_FILES, pendingFiles);

        String jobOperationName = "merge";
        Thread hook = newShutdownHook(jobOperationName, pendingFiles);
        Runtime.getRuntime().addShutdownHook(hook);
        try {
            mergeCalculateAlleles(studyId, pendingFiles, hadoopRoute, jar);
            mergeTransfer(studyId, pendingFiles, hadoopRoute, jar);

            setStatus(BatchFileOperation.Status.DONE, jobOperationName, pendingFiles);
        } catch (Exception e) {
            setStatus(BatchFileOperation.Status.ERROR, jobOperationName, pendingFiles);
            throw e;
        } finally {
            Runtime.getRuntime().removeShutdownHook(hook);
        }
    }

    private void mergeCalculateAlleles(int studyId, List<Integer> pendingFiles, String hadoopRoute, String jar) throws StorageEngineException {
        Class execClass = AlleleCalculatorDriver.class;
        String args = AlleleCalculatorDriver.buildCommandLineArgs(variantsTableCredentials.toString(),
                getArchiveTableName(),
                getCountTableName(),
                getCountTableName(), studyId, pendingFiles, options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();
        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading files {} into count table '{}'", pendingFiles, getVariantTableName());
        logger.info(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error merging allele count for files " + pendingFiles + " into count table \""
                    + getVariantTableName() + "\"");
        }
    }

    private void mergeTransfer(int studyId, List<Integer> pendingFiles, String hadoopRoute, String jar) throws StorageEngineException {
        Class execClass = AlleleTransferDriver.class;
        String args = AlleleTransferDriver.buildCommandLineArgs(variantsTableCredentials.toString(),
                getArchiveTableName(),
                getCountTableName(),
                getVariantTableName(), studyId, pendingFiles, options);
        String executable = hadoopRoute + " jar " + jar + ' ' + execClass.getName();

        long startTime = System.currentTimeMillis();
        logger.info("------------------------------------------------------");
        logger.info("Loading files {} into analysis table '{}'", pendingFiles, getVariantTableName());
        logger.info(executable + " " + args);
        logger.info("------------------------------------------------------");
        int exitValue = mrExecutor.run(executable, args);
        logger.info("------------------------------------------------------");
        logger.info("Exit value: {}", exitValue);
        logger.info("Total time: {}s", (System.currentTimeMillis() - startTime) / 1000.0);
        if (exitValue != 0) {
            throw new StorageEngineException("Error loading files " + pendingFiles + " into variant table \""
                    + getVariantTableName() + "\"");
        }
    }

    private String getArchiveTableName() {
        return archiveTableCredentials.getTable();
    }

    private String getVariantTableName() {
        return variantsTableCredentials.getTable();
    }

    private String getCountTableName() {
        return this.countTableName;
    }

    @Override
    protected void securePreMerge(StudyConfiguration studyConfiguration, VariantSource source) throws StorageEngineException {
        super.securePreMerge(studyConfiguration, source);
        // compatible
    }

}
