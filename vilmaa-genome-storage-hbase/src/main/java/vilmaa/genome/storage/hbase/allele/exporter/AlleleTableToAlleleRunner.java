/*
 * (C) Copyright 2018 VILMAA.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package vilmaa.genome.storage.hbase.allele.exporter;

import com.google.common.collect.BiMap;
import vilmaa.genome.storage.hbase.allele.AbstractLocalRunner;
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import vilmaa.genome.storage.models.samples.avro.SampleCollection;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;
import org.apache.avro.io.JsonEncoder;
import org.apache.avro.specific.SpecificDatumWriter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;

import java.io.File;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Created by mh719 on 24/02/2017.
 */
public class AlleleTableToAlleleRunner extends AbstractLocalRunner {
    public static final String OUTPUT_FILE = "vilmaa.allele.output.file";
//    private DataFileWriter<AlleleVariant> dataFileWriter;
    private HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;
    private Set<String> exportCohort;
    private Set<Integer> returnedSampleIds;
    private AvroParquetWriter<AlleleVariant> parquetWriter;
    private DataFileWriter dataFileWriter;

    @Override
    protected void map(Result result) throws IOException {
        AlleleVariant.Builder builder = this.hBaseAlleleCountsToAllelesConverter.convert(result);
        AlleleVariant avro = builder.build();
        this.dataFileWriter.append(avro);
//        this.parquetWriter.write(avro);
    }

    @Override
    protected void map(Scan scan, String variantTable) {
        prepareCohorts();
        prepareSampleFile();
        prepareConverter();
        try {
            prepareAvroWriter(() -> super.map(scan, variantTable));
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void prepareCohorts() {
        this.returnedSampleIds = new HashSet<>();
        StudyConfiguration sc = getStudyConfiguration();
        this.exportCohort = new HashSet<>(Arrays.asList(
                getConf().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));
        exportCohort.forEach((c) -> {
            if (!sc.getCohortIds().containsKey(c)) {
                throw new IllegalStateException("Cohort does not exist: " + c);
            }
            Integer id = sc.getCohortIds().get(c);
            this.returnedSampleIds.addAll(sc.getCohorts().getOrDefault(id, Collections.emptySet()));
        });
    }

    private void prepareSampleFile() {
        Path file = getSampleInfoOutputFile();
        StudyConfiguration sc = getStudyConfiguration();
        BiMap<Integer, String> idx = StudyConfiguration.getIndexedSamples(sc).inverse();
        SampleCollection collection = SampleCollection.newBuilder()
                .setSamples(this.returnedSampleIds.stream().collect(Collectors.toMap(s -> s, s-> idx.get(s))))
                .setCohorts(this.exportCohort.stream().collect(
                        Collectors.toMap(e -> e, e -> new ArrayList<>(sc.getCohorts().get(sc.getCohortIds().get(e))))))
                .build();
        try ( FileSystem fs = FileSystem.get(getConf());
              FSDataOutputStream out = fs.create(file, false)){
            DatumWriter<SampleCollection> writer = new GenericDatumWriter<>(SampleCollection.getClassSchema());
            JsonEncoder encoder = EncoderFactory.get().jsonEncoder(SampleCollection.getClassSchema(), out);
            writer.write(collection, encoder);
            encoder.flush();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private void prepareConverter() {
        StudyConfiguration sc = getStudyConfiguration();
        Set<Integer> availableSamples = StudyConfiguration.getIndexedSamples(sc).values();

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
        hBaseAlleleCountsToAllelesConverter.setCohortWhiteList(this.exportCohort);
    }

    protected void prepareParquetWriter(Runnable runnable) throws IOException {
        // http://blog.cloudera.com/blog/2014/05/how-to-convert-existing-data-into-parquet/
        Path path = getOutputFile();
        Schema classSchema = AlleleVariant.getClassSchema();
        CompressionCodecName codec = CompressionCodecName.SNAPPY;
        parquetWriter = new AvroParquetWriter<>(path, classSchema,
                codec, 1024, 1024, true, getConf());
        try {
            runnable.run();
        } finally {
            parquetWriter.close();
        }
    }

    protected void prepareAvroWriter(Runnable runnable) throws IOException {
        Path outputFile = getOutputFile();
        this.dataFileWriter = new DataFileWriter<>(
                new SpecificDatumWriter<>(AlleleVariant.class));
        try {
            dataFileWriter.create(AlleleVariant.SCHEMA$, new File(outputFile.toString()));
            runnable.run();
        } finally {
            dataFileWriter.close();
        }
    }

    private Path getOutputFile() {
        return checkFile(getConf().get(OUTPUT_FILE, StringUtils.EMPTY));
    }

    private Path getSampleInfoOutputFile() {
        String path = getConf().get(OUTPUT_FILE, StringUtils.EMPTY);
        checkFile(path);
        return checkFile(path + ".samples.json");
    }

    private Path checkFile(String outFile) {
        if (StringUtils.isBlank(outFile)) {
            throw new IllegalStateException("File output paramter required: " + OUTPUT_FILE);
        }
        Path path = new Path(outFile);
//        if (path.exists()) {
//            throw new IllegalStateException("File output already exists !!!");
//        }
//        if (!outFile.getParentFile().exists()) {
//            throw new IllegalStateException("File output directory does not exist !!!" + outFile.getParentFile());
//        }
        return path;
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
