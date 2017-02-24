package diva.genome.storage.hbase.allele.exporter;

import com.google.common.collect.BiMap;
import diva.genome.storage.hbase.allele.AbstractLocalRunner;
import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.samples.avro.SampleCollection;
import diva.genome.storage.models.samples.avro.SampleInformation;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Created by mh719 on 24/02/2017.
 */
public class AlleleTableToAlleleRunner extends AbstractLocalRunner {
    public static final String OUTPUT_FILE = "diva.allele.output.file";
    private DataFileWriter<AllelesAvro> dataFileWriter;
    private HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;
    private Set<String> exportCohort;
    private Set<Integer> returnedSampleIds;

    @Override
    protected void map(Result result) throws IOException {
        AllelesAvro.Builder builder = this.hBaseAlleleCountsToAllelesConverter.convert(result);
        AllelesAvro avro = builder.build();
        this.dataFileWriter.append(avro);
    }

    @Override
    protected void map(Scan scan, String variantTable) {
        prepareConverter();
        prepareSampleFile();
        try {
            prepareAvroWriter(() -> super.map(scan, variantTable));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void prepareSampleFile() {
        File file = getSampleInfoOutputFile();
        StudyConfiguration sc = getStudyConfiguration();
        BiMap<Integer, String> idx = StudyConfiguration.getIndexedSamples(sc).inverse();
        SampleCollection collection = SampleCollection.newBuilder()
                .setSamples(this.returnedSampleIds.stream()
                        .map(id -> SampleInformation.newBuilder().setSampleId(id).setSampleName(idx.get(id)).build())
                        .collect(Collectors.toList()))
                .build();
        try {
            FileUtils.write(file, collection.toString(), false);
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void prepareConverter() {
        this.returnedSampleIds = new HashSet<>();
        StudyConfiguration sc = getStudyConfiguration();
        Set<Integer> availableSamples = StudyConfiguration.getIndexedSamples(sc).values();
        this.exportCohort = new HashSet<>(Arrays.asList(
                getConf().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));
        exportCohort.forEach((c) -> {
            if (!sc.getCohortIds().containsKey(c)) {
                throw new IllegalStateException("Cohort does not exist: " + c);
            }
            Integer id = sc.getCohortIds().get(c);
            this.returnedSampleIds.addAll(sc.getCohorts().getOrDefault(id, Collections.emptySet()));
        });
        Set<Integer> invalid = this.returnedSampleIds.stream()
                .filter(k -> !availableSamples.contains(k)).collect(Collectors.toSet());
        if (!invalid.isEmpty()) {
            throw new IllegalStateException("Cohort sample(s) not indexed: " + invalid);
        }
        hBaseAlleleCountsToAllelesConverter = new HBaseAlleleCountsToAllelesConverter(getHelper(), sc);
        hBaseAlleleCountsToAllelesConverter.setReturnSampleIds(this.returnedSampleIds);
        hBaseAlleleCountsToAllelesConverter.setMutableSamplesPosition(true);
        hBaseAlleleCountsToAllelesConverter.setParseAnnotations(true);
        hBaseAlleleCountsToAllelesConverter.setParseStatistics(true);
    }

    protected void prepareAvroWriter(Runnable runnable) throws IOException {
        File outputFile = getOutputFile();
        this.dataFileWriter = new DataFileWriter<>(
                new SpecificDatumWriter<>(AllelesAvro.class));
        try {
            dataFileWriter.create(AllelesAvro.SCHEMA$, outputFile);
            runnable.run();
        } finally {
            dataFileWriter.close();
        }
    }

    private File getOutputFile() {
        return checkFile(getConf().get(OUTPUT_FILE, StringUtils.EMPTY));
    }

    private File getSampleInfoOutputFile() {
        String path = getConf().get(OUTPUT_FILE, StringUtils.EMPTY);
        checkFile(path);
        return checkFile(path + ".samples.json");
    }

    private File checkFile(String path) {
        if (StringUtils.isBlank(path)) {
            throw new IllegalStateException("File output paramter required: " + OUTPUT_FILE);
        }
        File outFile = new File(path);
        if (outFile.exists()) {
            throw new IllegalStateException("File output already exists !!!");
        }
        if (!outFile.getParentFile().exists()) {
            throw new IllegalStateException("File output directory does not exist !!!" + outFile.getParentFile());
        }
        return outFile;
    }

    public static void main(String[] args) throws Exception {
        try {
            System.exit(privateMain(args, new AlleleTableToAlleleRunner()));
        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }
}
