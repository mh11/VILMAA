package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.AbstractLocalRunner;
import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.specific.SpecificDatumWriter;
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
        AllelesAvro avro = this.hBaseAlleleCountsToAllelesConverter.convert(result);
        this.dataFileWriter.append(avro);
    }

    @Override
    protected void map(Scan scan, String variantTable) {
        prepareConverter();
        try {
            prepareAvroWriter(() -> super.map(scan, variantTable));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void prepareConverter() {
        StudyConfiguration sc = getStudyConfiguration();

        Set<Integer> availableSamples = StudyConfiguration.getIndexedSamples(sc).values();
        this.exportCohort = new HashSet<>(Arrays.asList(
                getConf().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));
        exportCohort.forEach((c) -> {
            if (sc.getCohortIds().containsKey(c)) {
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
        String outPath = getConf().get(OUTPUT_FILE, StringUtils.EMPTY);
        if (StringUtils.isBlank(outPath)) {
            throw new IllegalStateException("File output paramter required: " + OUTPUT_FILE);
        }
        File outFile = new File(outPath);
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
