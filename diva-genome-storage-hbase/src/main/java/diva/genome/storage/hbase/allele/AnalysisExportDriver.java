package diva.genome.storage.hbase.allele;

import com.fasterxml.jackson.databind.ObjectMapper;
import diva.genome.storage.hbase.allele.exporter.AlleleTableToAlleleMapper;
import diva.genome.storage.models.alleles.avro.AlleleVariant;
import org.apache.avro.mapreduce.AvroJob;
import org.apache.avro.mapreduce.AvroKeyOutputFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.io.compress.GzipCodec;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.opencb.opencga.storage.core.exceptions.StorageEngineException;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.IOException;
import java.util.Objects;

/**
 * Reads from Allele Variant table and creates an AVRO file in the format of {@link AlleleVariant}
 * Created by mh719 on 24/02/2017.
 */
public class AnalysisExportDriver extends AbstractAlleleDriver {
    public static final String CONFIG_ANALYSIS_EXPORT_PATH = "diva.genome.storage.allele.file";
    public static final String CONFIG_ANALYSIS_EXPORT_GENOTYPE = "diva.genome.storage.allele.genotype";
    public static final String CONFIG_ANALYSIS_EXPORT_COHORTS = "diva.genome.storage.allele.cohorts";
    public static final String CONFIG_ANALYSIS_EXPORT_MAF_COHORTS = "diva.genome.storage.allele.maf.cohorts";
    public static final String CONFIG_ANALYSIS_EXPORT_MAF_VALUE = "diva.genome.storage.allele.maf.cutoff";
    public static final String CONFIG_ANALYSIS_OPR_COHORTS = "diva.genome.storage.allele.opr.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_VALUE = "diva.genome.storage.allele.opr.cutoff";
    public static final String CONFIG_ANALYSIS_OPR_Y_COHORTS = "diva.genome.storage.allele.opr.y.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_Y_VALUE = "diva.genome.storage.allele.opr.y.cutoff";
    public static final String CONFIG_ANALYSIS_OPR_X_COHORTS = "diva.genome.storage.allele.opr.x.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_X_VALUE = "diva.genome.storage.allele.opr.x.cutoff";
    private String outAvroFile;

    @Override
    protected Class<? extends TableMapper> getMapperClass() {
        return AlleleTableToAlleleMapper.class;
    }

    @Override
    protected void parseAndValidateParameters() {
        super.parseAndValidateParameters();
        outAvroFile = null;
        if (!Objects.isNull(getConf().get(CONFIG_ANALYSIS_EXPORT_PATH, null))) {
            outAvroFile = getConf().get(CONFIG_ANALYSIS_EXPORT_PATH);
        }
        if (StringUtils.isBlank(this.outAvroFile)) {
            throw new IllegalArgumentException("No avro output path specified using " + CONFIG_ANALYSIS_EXPORT_PATH);
        }
    }

    @Override
    protected void initMapReduceJob(String inTable, Job job, Scan scan, boolean addDependencyJar) throws IOException {
        super.initMapReduceJob(inTable, job, scan, addDependencyJar);

        FileOutputFormat.setOutputPath(job, new Path(this.outAvroFile)); // set Path
        FileOutputFormat.setOutputCompressorClass(job, GzipCodec.class); // compression
        job.setOutputFormatClass(AvroKeyOutputFormat.class);
        AvroJob.setOutputKeySchema(job, AlleleVariant.getClassSchema()); // Set schema
        job.setNumReduceTasks(0);
    }

    @Override
    protected void postExecution(boolean succeed) throws IOException, StorageEngineException {
        super.postExecution(succeed);
        StudyConfiguration studyConfiguration = loadStudyConfiguration();
        writeMetadata(studyConfiguration, this.outAvroFile + ".studyConfiguration");
    }

    protected void writeMetadata(StudyConfiguration studyConfiguration, String output) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Path path = new Path(output);
        FileSystem fs = FileSystem.get(getConf());
        try (FSDataOutputStream fos = fs.create(path)) {
            objectMapper.writeValue(fos, studyConfiguration);
        }
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, null, new AnalysisExportDriver()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
