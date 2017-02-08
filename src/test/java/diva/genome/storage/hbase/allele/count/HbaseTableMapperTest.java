package diva.genome.storage.hadoop.allele.count;

import com.google.common.collect.BiMap;
import diva.genome.storage.hadoop.variant.index.VariantSliceMRTestHelper;
import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import diva.genome.storage.hbase.allele.count.HbaseTableMapper;
import diva.genome.storage.hbase.allele.transfer.HbaseTransferAlleleMapper;
import org.apache.commons.lang3.tuple.MutablePair;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Append;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Mapper;
import org.mockito.ArgumentCaptor;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.VariantSource;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.commons.datastore.core.QueryResult;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.metadata.StudyConfigurationManager;
import org.opencb.opencga.storage.core.variant.adaptors.VariantDBAdaptor;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.core.variant.stats.VariantStatisticsCalculator;
import org.opencb.opencga.storage.core.variant.stats.VariantStatsWrapper;
import org.opencb.opencga.storage.hadoop.allele.index.AlleleCombiner;
import org.opencb.opencga.storage.hadoop.allele.index.AlleleCountToHBaseConverter;
import org.opencb.opencga.storage.hadoop.allele.index.HBaseToAlleleCountConverter;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.OpencgaMapReduceHelper;
import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
import org.opencb.opencga.storage.hadoop.variant.index.AbstractVariantTableMapReduce;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyObject;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.*;

/**
 * Created by mh719 on 22/01/2017.
 */
public class HbaseTableMapperTest {

    @org.junit.Test
    public void check() throws IOException, InterruptedException {
        // 21:40:13 -> 21:43:14 (5000 samples) Found IDs:	10279940	Used Memory:	990 MB
        Set<Integer> tmpList = new HashSet<>();

        VariantSliceMRTestHelper helper = new VariantSliceMRTestHelper(new File
//                ("/Users/mh719/data/proto/10_000000125014.local.2"));
//                ("/Users/mh719/data/proto/bridge12_1_000000138157"));
                ("/Users/mh719/data/proto/bridge3_10_000000125014"));
//        ("/Users/mh719/data/proto/chunk_6_155721")); // sampleid 13204 fileId 21955


//        Set<Integer> restrictedFileIds = VariantSliceMRTestHelper.buildBatchFileIds(helper.loadConfiguration());
//        Set<Integer> restrictedFileIds = Collections.singleton(21946);
        Set<Integer> restrictedFileIds = new HashSet<>(Arrays.asList(
                helper.loadConfiguration().getSamplesInFiles().keySet().toArray(new Integer[0])).subList(0, 5000));

//        String sIdParam = "LP2000747-DNA_H10,LP2000748-DNA_A01,LP2000747-DNA_H11,LP2000748-DNA_A02,LP2000748-DNA_A03,"
//                + "LP2000748-DNA_A04,LP2000748-DNA_A05,LP2000748-DNA_A07,LP2000748-DNA_A08";
//        Set<String> ids = Arrays.stream((sIdParam).split(",")).collect(Collectors.toSet());
//        Set<Integer> tmpSampleIds = helper.loadConfiguration().getSampleIds().entrySet().stream().filter(e -> ids
// .contains
//                (e.getKey()))
//                .map(e -> e.getValue()).collect(Collectors.toSet());
//
//        Set<Integer> restrictedFileIds = helper.loadConfiguration().getSamplesInFiles().entrySet().stream()
//                .filter(e -> e.getValue().stream().anyMatch(i -> tmpSampleIds.contains(i)))
//                .map(e -> e.getKey())
//                .collect(Collectors.toSet());

        Set<Integer> restictedSampleIds = helper.loadConfiguration().getSamplesInFiles().entrySet().stream()
                .filter(e -> restrictedFileIds.contains(e.getKey()))
                .flatMap(e -> e.getValue().stream()).collect(Collectors.toSet());
        restictedSampleIds.remove(18642);

//                restictedSampleIds.addAll(VariantSliceMRTestHelper.buildBatchSampleIds(helper.loadConfiguration()));

        System.out.println("Load " + restrictedFileIds.size() + " files with " + restictedSampleIds.size() + " sample"
                + " ids ... ");
        helper.load(restictedSampleIds);
        StudyConfiguration studyConfiguration = helper.getStudyConfiguration();
        byte[] columnFamily = helper.getGenomeHelper().getColumnFamily();

        Set<Integer> sids = restictedSampleIds;
        if (sids.isEmpty()) {
            sids = helper.buildBatchSampleIds();
        }

        BiMap<Integer, String> sampleIdToNames = studyConfiguration.getSampleIds().inverse();
        Set<String> restictedNames = restictedSampleIds.stream().map(i -> sampleIdToNames.get(i))
                .collect(Collectors.toSet());

        Mapper.Context context = mock(Mapper.Context.class);
        when(context.getConfiguration()).thenReturn(helper.getConfig());
        when(context.getCounter(anyString(), anyString())).thenReturn(mock(Counter.class));
        when(context.getCounter(anyObject())).thenReturn(mock(Counter.class));

        AbstractVariantTableMapReduce.VariantMapReduceContext ctx =
                new AbstractVariantTableMapReduce.VariantMapReduceContext(
                        helper.getRowKey(), context, helper.buildInputRow(), helper.getLastBatchFiles(),
                        sids, helper.getChromosome(), helper.getStart(), helper.getNextStart()
                );


//        // Genome Helper
        VariantTableHelper ghSpy = spy(helper.getGenomeHelper());
//        HBaseManager hbm = mock(HBaseManager.class);
//        when(ghSpy.getHBaseManager()).thenReturn(hbm);
//        byte[] intputTable = helper.getGenomeHelper().getIntputTable();
//        when(hbm.act(eq(intputTable), anyObject())).thenReturn(helper.buildArchiveRow());
//
        StudyConfigurationManager scm = mock(StudyConfigurationManager.class);
        when(scm.getStudyConfiguration(anyInt(), anyObject())).thenReturn(
                new QueryResult<>("a", -1, -1, -1l, "", "",
                        Collections.singletonList(studyConfiguration)));

//        HBaseToVariantConverter variantConverter = new HBaseToVariantConverter(ghSpy, scm);
//        variantConverter.setFailOnEmptyVariants(true);

        HBaseTableMapperDummy mapper = new HBaseTableMapperDummy();
        mapper.setSampleNameToSampleId(studyConfiguration.getSampleIds());
        ArchiveResultToVariantConverter resultConverter = new ArchiveResultToVariantConverter(
                studyConfiguration.getStudyId(), columnFamily, studyConfiguration);
        mapper.setResultConverter(resultConverter);
        resultConverter.setParallel(false);

//        mapper.processCells(helper.buildInputRow().listCells(), f -> {});

        mapper.setMrHelper(new OpencgaMapReduceHelper(context));
        mapper.setStudyConfiguration(studyConfiguration);
        mapper.setHelper(ghSpy);
        /* ASYNC stuff */
        mapper.setAsyncPut(true);
        mapper.setupAsyncQueue(f -> {
        }, f -> {
        });


////        ForkJoinPool pool = VariantTableMapper.createForkJoinPool("MyLocaLBla", 100);
//        mapper.setHbaseToVariantConverter(variantConverter);
//        mapper.variantMerger = null;
//        mapper.setTimestamp(TIMESTAMP);
//        mapper.setIndexedSamples(HashBiMap.create());
        mapper.setCurrentIndexingSamples(restictedNames);
//        mapper.archiveBatchSize = 1000;


//
        // RUN
        try {
            mapper.doMap(ctx);
        } finally {
            mapper.cleanupAsyncQueue();
        }

//        if (true) {
//            System.exit(1);
//        }
        // Post processing
        studyConfiguration.getIndexedFiles().clear();
        studyConfiguration.getIndexedFiles().addAll(restrictedFileIds);

        List<Result> inputQuery = mapper.appends.stream()
                .map(i -> Result.create(i.getFamilyCellMap().get(columnFamily)))
                .sorted((a, b) -> Bytes.compareTo(a.getRow(), b.getRow()))
                .collect(Collectors.toList());

        HbaseTransferAlleleMapper alleleMapper = new TransferTestMapper();
        alleleMapper.setMrHelper(new OpencgaMapReduceHelper(context));
        alleleMapper.setHelper(ghSpy);

////        ForkJoinPool pool = VariantTableMapper.createForkJoinPool("MyLocaLBla", 100);
//        alleleMapper.setTimestamp(TIMESTAMP);
        alleleMapper.setStudyConfiguration(studyConfiguration);
        alleleMapper.setIndexedSamples(StudyConfiguration.getIndexedSamples(studyConfiguration));
        alleleMapper.alleleCountConverter = new HBaseToAlleleCountConverter();
        alleleMapper.setStudiesRow(ghSpy.generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0));

        AtomicInteger iter = new AtomicInteger(-1);

        when(context.nextKeyValue()).then(a -> iter.incrementAndGet() < inputQuery.size());
        when(context.getCurrentKey()).then(a -> new ImmutableBytesWritable(inputQuery.get(iter.get()).getRow()));
        when(context.getCurrentValue()).then(a -> inputQuery.get(iter.get()));

        // Capture
        ArgumentCaptor<ImmutableBytesWritable> immuteCapture = ArgumentCaptor.forClass(ImmutableBytesWritable.class);
        ArgumentCaptor<Put> putCapture = ArgumentCaptor.forClass(Put.class);

        alleleMapper.alleleCombiner = new AlleleCombiner(restictedSampleIds);
        alleleMapper.converter = new AlleleCountToHBaseConverter(helper.getGenomeHelper().getColumnFamily(),
                studyConfiguration.getStudyId() + "");
        alleleMapper.run(context);
        verify(context, atLeastOnce()).write(immuteCapture.capture(), putCapture.capture());


/*  Convert PUT to VARIANT      */

//        StudyConfigurationManager scmPost = mock(StudyConfigurationManager.class);
//        when(scmPost.getStudyConfiguration(anyInt(), anyObject())).thenReturn(
//                new QueryResult<>("a",-1,-1,-1l, "","",Collections.singletonList(studyConfiguration)));
        HBaseAlleleCountsToVariantConverter variantConverterPost =
                new HBaseAlleleCountsToVariantConverter(helper.getGenomeHelper(), studyConfiguration);
        variantConverterPost.setReturnSampleIds(restictedSampleIds);
        variantConverterPost.setReturnSamples(Collections.singleton("."));


        List<Put> otherValues = putCapture.getAllValues();
//        List<ImmutableBytesWritable> otherImmute = immuteCapture.getAllValues();

        VariantStatisticsCalculator variantStatisticsCalculator = new VariantStatisticsCalculator(true);
        variantStatisticsCalculator.setAggregationType(VariantSource.Aggregation.NONE, null);

        BiMap<Integer, String> sampleIds = studyConfiguration.getSampleIds().inverse();

        Map<String, Set<String>> samples = studyConfiguration.getCohortIds().entrySet().stream()
                .map(e -> new MutablePair<>(e.getKey(), studyConfiguration.getCohorts().get(e.getValue())))
                .map(p -> new MutablePair<>(
                        p.getKey(),
                        p.getValue().stream().map(i -> sampleIds.get(i)).collect(Collectors.toSet())))
                .collect(Collectors.toMap(p -> p.getKey(), p -> p.getValue()));


        List<Variant> recoveredVariants = new ArrayList<>();
        int cnt = 0;
        for (Put put : otherValues) {
            NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
            List<Cell> cells = familyCellMap.get(columnFamily);
            Result result = Result.create(cells);
            Variant rowKey = variantConverterPost.convertRowKey(result.getRow());
            if (!rowKey.getType().equals(VariantType.NO_VARIATION)) {
                Variant convert = variantConverterPost.convert(result);
                if (null != convert && !convert.getType().equals(VariantType.NO_VARIATION)) {
                    recoveredVariants.add(convert);
                    List<VariantStatsWrapper> stats = variantStatisticsCalculator.calculateBatch(
                            Collections.singletonList(convert), helper.getGenomeHelper().getStudyId() + "",
                            "notused", samples);
                    System.out.println(convert);
                    ++cnt;
                }
            }
        }
        System.out.println("cnt = " + cnt);
        System.out.println(otherValues.size());

        QueryOptions options = new QueryOptions();
//        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), StringUtils.join(restictedSampleIds
// .stream().sorted().collect(Collectors.toList()), ","));
//        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), 18249);
//        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), sIdParam);
        options.add(VariantDBAdaptor.VariantQueryParams.RETURNED_SAMPLES.key(), ".");

        VariantSourceDBAdaptor source = mock(VariantSourceDBAdaptor.class);
        when(source.iterator(anyObject(), anyObject())).thenReturn(helper.getVariantSourceList().iterator());
        OutputStream out = System.out;
        try {
//        try(OutputStream out = new FileOutputStream(new File(dir.getAbsolutePath() + ".vcf"))) {
            VariantVcfDataWriter exporter = new VariantVcfDataWriter(studyConfiguration, source, out, options);
            exporter.setExportGenotype(false);
//            exporter.setSampleNameConverter(s -> s+"_XXX");
            exporter.open();
            exporter.pre();
            for (Variant variant : recoveredVariants) {
                exporter.write(Collections.singletonList(variant));
            }
            exporter.post();
//            exporter.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static class HBaseTableMapperDummy extends HbaseTableMapper {
        private List<Append> appends = new ArrayList<>();
        @Override
        public void doSubmit(Collection<Append> appends) throws IOException {
            this.appends.addAll(appends);
            getLog().info("Received {} appends; total {} ", appends.size(), this.appends.size());
        }

        public void setResultConverter(ArchiveResultToVariantConverter resultConverter) {
            this.resultConverter = resultConverter;
        }

        public void setCurrentIndexingSamples(Set<String> currentIndexingSamples) {
            this.currentIndexingSamples = currentIndexingSamples;
        }
    }

    public static class TransferTestMapper extends HbaseTransferAlleleMapper {
        private List<Append> appends = new ArrayList<>();



        @Override
        protected void setup(Mapper.Context context) throws IOException, InterruptedException {

        }

        @Override
        protected void cleanup(Mapper.Context context) throws IOException, InterruptedException {

        }
    }




}