package diva.genome.storage.hbase.allele.opencga;

import org.apache.commons.lang3.NotImplementedException;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.opencb.commons.datastore.core.ObjectMap;
import org.opencb.opencga.storage.core.StoragePipelineResult;
import org.opencb.opencga.storage.core.config.StorageEngineConfiguration;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.exceptions.StoragePipelineException;
import org.opencb.opencga.storage.core.variant.VariantStorageEngine;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.annotation.VariantAnnotationManager;
import org.opencb.opencga.storage.core.variant.annotation.annotators.VariantAnnotator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsManager;
import org.opencb.opencga.storage.hadoop.auth.HBaseCredentials;
import org.opencb.opencga.storage.hadoop.variant.AbstractHadoopVariantStoragePipeline;
import org.opencb.opencga.storage.hadoop.variant.HadoopVariantStorageEngine;
import org.opencb.opencga.storage.hadoop.variant.adaptors.VariantHadoopDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveDriver;

import java.io.IOException;
import java.net.URI;
import java.util.List;
import java.util.Map;

/**
 * Created by mh719 on 15/02/2017.
 */
public class HbaseVariantStorageEngine extends HadoopVariantStorageEngine {


    public static final String DIVA_GENOME_ALLELE_VARIANT_TABLE_NAME = "diva.genome.allele.variant.table.name";

    @Override
    public void dropFile(String study, int fileId) throws StorageEngineException {
        throw new NotImplementedException("");
    }

    @Override
    public List<StoragePipelineResult> index(List<URI> inputFiles, URI outdirUri, boolean doExtract, boolean
            doTransform, boolean doLoad) throws StorageEngineException {
        return super.index(inputFiles, outdirUri, doExtract, doTransform, doLoad);
    }

    @Override
    public VariantHadoopDBAdaptor getDBAdaptor(String tableName) throws StorageEngineException {
        String variantTableName = getVariantTableName();
        logger.warn("Ignore table name {} -> use {} instead.", tableName, variantTableName);
        return getDBAdaptor(buildCredentials(variantTableName));
    }

    @Override
    protected VariantHadoopDBAdaptor getDBAdaptor(HBaseCredentials credentials) throws StorageEngineException {
        try {
            StorageEngineConfiguration storageEngine = this.configuration.getStorageEngine(STORAGE_ENGINE_ID);
            Configuration configuration = getHadoopConfiguration(storageEngine.getVariant().getOptions());
            configuration = VariantHadoopDBAdaptor.getHbaseConfiguration(configuration, credentials);
            return new HBaseVariantDBAdaptor(getHBaseManager(configuration).getConnection(), credentials,
                    this.configuration, configuration);
        } catch (IOException e) {
            throw new StorageEngineException("Problems creating DB Adapter", e);
        }
    }

    @Override
    public AbstractHadoopVariantStoragePipeline newStorageETL(boolean connected, Map<? extends String, ?> extraOptions)
            throws StorageEngineException {
        ObjectMap options = new ObjectMap(configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions());
        if (extraOptions != null) {
            options.putAll(extraOptions);
        }
        if (!options.getBoolean(HADOOP_LOAD_DIRECT, HADOOP_LOAD_DIRECT_DEFAULT)) {
            throw new StorageEngineException("Only direct loading supported!");
        }
        VariantHadoopDBAdaptor dbAdaptor = connected ? getDBAdaptor() : null;
        Configuration hadoopConfiguration = null == dbAdaptor ? null : dbAdaptor.getConfiguration();
        hadoopConfiguration = hadoopConfiguration == null ? getHadoopConfiguration(options) : hadoopConfiguration;
        hadoopConfiguration.setIfUnset(ArchiveDriver.CONFIG_ARCHIVE_TABLE_COMPRESSION, Compression.Algorithm.SNAPPY.getName());

        HBaseCredentials archiveCredentials = buildCredentials(getArchiveTableName(options.getInt(Options.STUDY_ID.key()), options));

        return new HbaseVariantStoragePipeline(configuration, storageEngineId, dbAdaptor, getMRExecutor(options),
                    hadoopConfiguration, archiveCredentials, getVariantReaderUtils(hadoopConfiguration), options);
    }

    @Override
    public VariantStatisticsManager newVariantStatisticsManager(VariantDBAdaptor dbAdaptor) {
        return super.newVariantStatisticsManager(dbAdaptor);
    }

    @Override
    protected VariantAnnotationManager newVariantAnnotationManager(VariantAnnotator annotator, VariantDBAdaptor
            dbAdaptor) {
        return super.newVariantAnnotationManager(annotator, dbAdaptor);
    }

    @Override
    public AbstractHadoopVariantStoragePipeline newStoragePipeline(boolean connected) throws StorageEngineException {
        return super.newStoragePipeline(connected); // points to newStorageETL
    }

    @Override
    protected void annotateLoadedFiles(URI outdirUri, List<URI> files, List<StoragePipelineResult> results, ObjectMap
            options) throws StoragePipelineException {
        super.annotateLoadedFiles(outdirUri, files, results, options); // maybe be careful where to point to
    }

    @Override
    protected void calculateStatsForLoadedFiles(URI output, List<URI> files, List<StoragePipelineResult> results, ObjectMap options) throws StoragePipelineException {
        super.calculateStatsForLoadedFiles(output, files, results, options); // maybe be careful where to point to
    }

    @Override
    public String getVariantTableName() {
        String name = configuration.getStorageEngine(STORAGE_ENGINE_ID).getVariant().getOptions().getString
                (DIVA_GENOME_ALLELE_VARIANT_TABLE_NAME, "");
        if (StringUtils.isBlank(name)) {
            throw new IllegalStateException("Please specify a variant table name using "
                    + DIVA_GENOME_ALLELE_VARIANT_TABLE_NAME);
        }
        this.getOptions().put(VariantStorageEngine.Options.DB_NAME.key(), name);
        return getVariantTableName(name);
    }
}
