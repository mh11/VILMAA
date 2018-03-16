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

package vilmaa.genome.storage.hbase.variant.index;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.protobuf.VcfMeta;
import org.opencb.biodata.models.variant.protobuf.VcfSliceProtos;
import org.opencb.opencga.storage.core.metadata.BatchFileOperation;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;

/**
 * Created by mh719 on 26/01/2017.
 */
public class VariantSliceMRTestHelper {

    private final File directory;
    private final List<File> inputFiles;
    private final List<File> vData;
    private final Configuration config;
    private final List<KeyValue> archiveKv = new ArrayList<>();
    private final List<KeyValue> kv = new ArrayList<>();
    private final List<VariantSource> variantSourceList = new ArrayList<>();

    private StudyConfiguration studyConfiguration;
    private LinkedHashSet<Integer> lastBatchFiles = new LinkedHashSet<>();
    private List<byte[]> loadedSlices = new ArrayList<>();
    private VariantTableHelper gh;
    private String chromosome;
    private Integer position;

    public VariantSliceMRTestHelper(File directory) {
        this.directory = directory;
        this.config = new Configuration();
        this.inputFiles = Arrays.stream(directory.listFiles()).filter(f -> f.getName().endsWith("variants.proto.gz"))
                .collect(Collectors.toList());
        this.vData = Arrays.stream(directory.listFiles()).filter(f -> f.getName().startsWith("_V_"))
                .collect(Collectors.toList());
    }


    public void load() throws IOException {
        load(Collections.emptySet());
    }

    public void load(Set<Integer> sampleIds) throws IOException {
        this.studyConfiguration = this.loadConfiguration();
        VariantTableHelper.setStudyId(config, studyConfiguration.getStudyId());
        lastBatchFiles.addAll(this.studyConfiguration.lastBatch().getFileIds());
        this.gh = new VariantTableHelper(config, "intable", "outtable", null);
        loadSlices(sampleIds);
    }

    private void loadSlices(Set<Integer> sampleIds) {
        LinkedHashSet<Integer> targetIds = this.lastBatchFiles;
        Map<Integer, VcfMeta> conf = new HashMap<>();

        ObjectMapper objectMapper = new ObjectMapper();
        Set<String> chrSet = new HashSet<>();
        Set<Integer> posSet = new HashSet<>();
        this.inputFiles.stream().forEach(file -> {
            File confFile = new File(file.getAbsolutePath().replace("vcf.gz.variants.proto.gz","vcf.gz.file.json.gz"));
            Integer fid = Integer.valueOf(confFile.getName().replace(".vcf.gz.file.json.gz",""));
            LinkedHashSet<Integer> samples = studyConfiguration.getSamplesInFiles().get(fid);
            if (!sampleIds.isEmpty()) {
                HashSet<Integer> set = new HashSet<>(samples);
                set.retainAll(sampleIds);
                if (set.isEmpty()) {
                    return;
                }
            }
            targetIds.add(fid);
            try ( InputStream in = new GZIPInputStream(new FileInputStream(file));
                  InputStream inconf = new GZIPInputStream(new FileInputStream(confFile)); ) {
                byte[] bytes = IOUtils.toByteArray(in);
                this.loadedSlices.add(bytes);
                VcfSliceProtos.VcfSlice vcfSlice = VcfSliceProtos.VcfSlice.parseFrom(bytes);
                chrSet.add(vcfSlice.getChromosome());
                posSet.add(vcfSlice.getPosition());
                KeyValue keyValue = new KeyValue(gh.generateVariantRowKey(vcfSlice.getChromosome(), vcfSlice
                        .getPosition()), gh.getColumnFamily(), Bytes.toBytes(fid.toString()), vcfSlice.toByteArray());

                if (targetIds.contains(fid)){
                    kv.add(keyValue);
                }
                if (studyConfiguration.getIndexedFiles().contains(fid)) {
                    archiveKv.add(keyValue);
                }
                byte[] confArr = IOUtils.toByteArray(inconf);
                VariantSource vs = objectMapper.readValue(confArr, VariantSource.class);
                vs.setStudyId(studyConfiguration.getStudyId() + "");
                variantSourceList.add(vs);
                conf.put(fid, new VcfMeta(vs));
            } catch (IOException e) {
                throw new IllegalStateException(e.getMessage(), e);
            }
        });
        this.chromosome = chrSet.stream().findFirst().get();
        this.position = posSet.stream().findFirst().get();
    }

    public StudyConfiguration loadConfiguration() throws IOException {
        File[] files = this.directory.listFiles(f -> StringUtils.equals(f.getName(), "studycolumn"));
        if (files.length > 0) {
            ObjectMapper objectMapper = new ObjectMapper();
            try ( FileInputStream in = new FileInputStream(files[0]) ) {
                return objectMapper.readValue(IOUtils.toByteArray(in), StudyConfiguration.class);
            }
        }

        ObjectMapper objectMapper = new ObjectMapper();
        StudyConfiguration studyConfiguration = new StudyConfiguration(2, "2");

        List<VariantSource> variantSourceList = new ArrayList<>();
        Map<String, Integer> sampleIds = new HashMap<>();
        Map<Integer, LinkedHashSet<Integer>> samplesInFile = new HashMap<>();
        Map<String, Integer> fileIds = new HashMap<>();
        LinkedHashSet<Integer> indexedFiles = new LinkedHashSet<>();
        // else load from meta
        Arrays.stream(this.directory.listFiles(f -> StringUtils.endsWith(f.getName(), "variants.proto.gz"))).forEach(file -> {
            File confFile = new File(file.getAbsolutePath().replace("vcf.gz.variants.proto.gz", "vcf.gz.file.json.gz"));
            Integer fid = Integer.valueOf(confFile.getName().replace(".vcf.gz.file.json.gz", ""));
            try (InputStream inconf = new GZIPInputStream(new FileInputStream(confFile)); ) {
                byte[] confArr = IOUtils.toByteArray(inconf);
                VariantSource vs = objectMapper.readValue(confArr, VariantSource.class);
                vs.setStudyId(studyConfiguration.getStudyId() + "");
                variantSourceList.add(vs);

                fileIds.put(vs.getFileName(), fid);
                sampleIds.put(vs.getSamples().get(0), fid);
                samplesInFile.put(fid, new LinkedHashSet<>(Collections.singleton(fid)));
                indexedFiles.add(fid);
            } catch (IOException e) {
                throw new IllegalStateException(e);
            }
        });

        // remove some to merge

        indexedFiles.removeAll(Arrays.asList(indexedFiles.toArray(new Integer[0])).subList(indexedFiles.size()-10,indexedFiles.size()));
        studyConfiguration.setSampleIds(sampleIds);
        studyConfiguration.setSamplesInFiles(samplesInFile);
        studyConfiguration.setFileIds(fileIds);
        studyConfiguration.setIndexedFiles(indexedFiles);
        // create fake batch file
        List<Integer> batchFiles = fileIds.values().stream().filter(v -> !indexedFiles.contains(v)).collect(Collectors.toList());
        batchFiles = batchFiles.subList(0, Math.min(batchFiles.size()-1, 10));
        long timeStamp = 2;
        String operation = "";
        studyConfiguration.setBatches(Collections.singletonList(new BatchFileOperation(operation, batchFiles, timeStamp, BatchFileOperation.Type.LOAD)));
        return studyConfiguration;
    }

    public Result buildInputRow() {
        return new Result(kv);
    }

    public Result buildArchiveRow() {
        return new Result(archiveKv);
    }

    public byte[] getRowKey() {
        return gh.generateVariantRowKey(getChromosome(), getStart());
    }

    public String getChromosome() {
        return chromosome;
    }

    public Integer getStart() {
        return position;
    }

    public Integer getNextStart() {
        return position + 1000;
    }

    public List<VariantSource> getVariantSourceList() {
        return variantSourceList;
    }

    public Configuration getConfig() {
        return config;
    }

    public LinkedHashSet<Integer> getLastBatchFiles() {
        return lastBatchFiles;
    }

    public Set<Integer> buildBatchSampleIds() {
        return studyConfiguration.getSamplesInFiles().entrySet().stream().filter(e -> {
            for (Integer fids : lastBatchFiles) {
                if (e.getValue().contains(fids)){
                    return true;
                }
            }
            return false;
        }).map(e -> e.getKey()).collect(Collectors.toSet());
    }

    public static Set<Integer> buildBatchSampleIds(StudyConfiguration configuration) {
        BatchFileOperation batchFileOperation = configuration.getBatches().get(configuration.getBatches().size() - 1);
        return configuration.getSamplesInFiles().entrySet().stream().filter(e -> {
            for (Integer fids : batchFileOperation.getFileIds()) {
                if (e.getValue().contains(fids)){
                    return true;
                }
            }
            return false;
        }).map(e -> e.getKey()).collect(Collectors.toSet());
    }

    public static Set<Integer> buildBatchFileIds(StudyConfiguration configuration) {
        return new HashSet<>(configuration.getBatches().get(configuration.getBatches().size() - 1).getFileIds());
    }

    public Set<String> buildBatchSampleNames() {
        return buildBatchSampleIds().stream().map(sid -> studyConfiguration.getSampleIds().inverse().get(sid))
                .collect(Collectors.toSet());
    }

    public VariantTableHelper getGenomeHelper() {
        return gh;
    }

    public StudyConfiguration getStudyConfiguration() {
        return studyConfiguration;
    }

    public List<byte[]> getLoadedSlices() {
        return this.loadedSlices;
    }
}
