package vilmaa.genome.storage.hbase.allele.count;

//import com.google.common.collect.BiMap;
//import HBaseAlleleCalculator;
//import HBaseAlleleTransfer;
//import diva.genome.storage.hbase.allele.count.region.*;
//import VariantSliceMRTestHelper;
//import PointRegion;
//import org.apache.commons.lang3.tuple.ImmutablePair;
//import org.apache.commons.lang3.tuple.Pair;
//import org.apache.hadoop.hbase.client.Append;
//import org.apache.hadoop.hbase.client.Result;
//import org.opencb.biodata.models.variant.Variant;
//import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
//import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
//import org.opencb.opencga.storage.hadoop.variant.archive.ArchiveResultToVariantConverter;
//import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;
//import org.opencb.opencga.storage.hadoop.variant.index.VariantTableMapper;
//
//import java.io.File;
//import java.io.IOException;
//import java.util.*;
//import java.util.function.Consumer;
//import java.util.stream.Collectors;

/**
 * Created by mh719 on 21/03/2017.
 */
public class HbaseTableLocalTest {

//    @org.junit.Test
//    public void check() throws IOException, InterruptedException {
//        VariantSliceMRTestHelper helper = new VariantSliceMRTestHelper(new File
//                ("/Users/mh719/data/proto/bridge3_10_000000125014"));
//        Set<Integer> restrictedFileIds = new HashSet<>(Arrays.asList(
//                helper.loadConfiguration().getIndexedFiles().toArray(new Integer[0])).subList(0, 50));
//
//        Set<Integer> restictedSampleIds = helper.loadConfiguration().getSamplesInFiles().entrySet().stream()
//                .filter(e -> restrictedFileIds.contains(e.getKey()))
//                .flatMap(e -> e.getValue().stream()).collect(Collectors.toSet());
//        restictedSampleIds.remove(18642);
//
//        System.out.println("Load " + restrictedFileIds.size() + " files with " + restictedSampleIds.size() + " sample"
//                + " ids ... ");
//        helper.load(restictedSampleIds);
//        StudyConfiguration studyConfiguration = helper.getStudyConfiguration();
//        VariantTableHelper genomeHelper = helper.getGenomeHelper();
//        byte[] columnFamily = genomeHelper.getColumnFamily();
//
//        Set<Integer> sids = restictedSampleIds;
//        if (sids.isEmpty()) {
//            sids = helper.buildBatchSampleIds();
//        }
//        System.out.println("sids = " + sids);
//
//        BiMap<Integer, String> sampleIdToNames = studyConfiguration.getSampleIds().inverse();
//        Map<String, Integer> restictedNames = restictedSampleIds.stream().collect(Collectors.toMap(i -> sampleIdToNames.get(i), i -> i));
//
//
//        HBaseAlleleCalculator alleleCalculator = new HBaseAlleleCalculator(genomeHelper.getStudyId() + "", restictedNames);
//        AlleleRegionCalculator regionCalculator = new AlleleRegionCalculator(genomeHelper.getStudyId() + "", restictedNames, helper.getStart(), helper.getNextStart() - 1);
//
//        System.out.println("Count alleles");
//        process(studyConfiguration, genomeHelper, helper.buildInputRow(), variant -> {
//            alleleCalculator.addVariant(variant);
//            regionCalculator.addVariant(variant);
//        });
//        System.out.println("Fill No Calls");
//        alleleCalculator.fillNoCalls(restictedNames.keySet(), helper.getStart(), helper.getNextStart());
//        alleleCalculator.onlyLeaveSparseRepresentation(helper.getStart(), helper.getNextStart(), false, false);
//
//        regionCalculator.fillNoCalls(restictedNames.keySet(), helper.getStart(), helper.getNextStart());
//
//
//        System.out.println("Merge ...");
//        Map<Integer, AlleleCountPosition> alleleCountRefMap = alleleCalculator.buildReferenceMap();
//        Map<Integer, Map<String, AlleleCountPosition>> alleleCountVarMap = alleleCalculator.buildVariantMap();
//
//        HBaseAlleleTransfer hBaseAlleleTransfer = new HBaseAlleleTransfer(restictedSampleIds);
//        HBaseAlleleRegionTransfer hBaseAlleleRegionTransfer = new HBaseAlleleRegionTransfer(restictedSampleIds);
//
//        AlleleRegionStore store = buildStore(helper.getChromosome(), regionCalculator, genomeHelper);
//
//        List<Integer> refPositions = new ArrayList<>(alleleCountRefMap.keySet());
//        Collections.sort(refPositions);
//        List<Pair<AlleleCountPosition, Variant>> positionBuffer = new ArrayList<>();
//        for (Integer position : refPositions) {
//            positionBuffer.clear();
//            if (!alleleCountVarMap.containsKey(position)){
//                continue;
//            }
//            AlleleCountPosition refBean = alleleCountRefMap.get(position);
//            Map<String, AlleleCountPosition> varMap = alleleCountVarMap.get(position);
//            varMap.forEach((vid, cnt) -> positionBuffer.add(new ImmutablePair<>(cnt, HBaseAlleleRegionTransfer.buildVariant(helper.getChromosome(), position, vid))));
//            List<Pair<Variant, AlleleCountPosition>> oldImpl = new ArrayList<>();
//            hBaseAlleleTransfer.process(refBean, positionBuffer, (v, c) -> oldImpl.add(new ImmutablePair<>(v, c)));
//            List<Pair<Variant, AlleleCountPosition>> newImpl = new ArrayList<>();
//            hBaseAlleleRegionTransfer.transfer(helper.getChromosome(), new PointRegion(null, position), store, (v, c) -> {if (v.getStart().equals(position)) {newImpl.add(new ImmutablePair<>(v, c));}});
//            if (oldImpl.size() != newImpl.size()) {
//                System.out.println("oldImpl = " + oldImpl);
//                System.out.println("newImpl = " + newImpl);
//            }
//            oldImpl.forEach(op -> {
//                List<Pair<Variant, AlleleCountPosition>> collect = newImpl.stream().filter(p -> p.getKey().equals(op
//                        .getKey())).collect(Collectors.toList());
//                if (collect.size() != 1) {
//                    System.out.println(op.getKey() + ": op = " + op);
//                }
//                Pair<Variant, AlleleCountPosition> np = collect.get(0);
//                if (isNotEquals(op.getValue().getAlternate(), np.getValue().getAlternate())) {
//                    System.out.println(op.getKey() + ": op.getValue().getAlternate() = " + op.getValue().getAlternate());
//                    System.out.println(np.getKey() + ": np.getValue().getAlternate() = " + np.getValue().getAlternate());
//                }
//                if (isNotEquals(op.getValue().getReference(), np.getValue().getReference())) {
//                    System.out.println(op.getKey() + ": op.getValue().getReference().size = " + op.getValue().getReference().size());
//                    System.out.println(np.getKey() + ": np.getValue().getReference().size() = " + np.getValue().getReference().size());
////                    System.out.println(op.getValue().getReference().get(0).stream().filter(i -> !np.getValue().getReference().get(0).contains(i)).collect(Collectors.toList()));
////                    System.out.println(np.getValue().getReference().get(0).stream().filter(i -> !op.getValue().getReference().get(0).contains(i)).collect(Collectors.toList()));
//                }
//                if (isNotEqualsAlt(op.getValue().getAltMap(), np.getValue().getAltMap())) {
//                    System.out.println(op.getKey() + ": op.getValue().getAltMap() = " + op.getValue().getAltMap());
//                    System.out.println(np.getKey() + ": np.getValue().getAltMap() = " + np.getValue().getAltMap());
//                }
//                if (isNotEquals(op.getValue().getNotPass(), np.getValue().getNotPass())) {
//                    System.out.println(op.getKey() + ": op.getValue().getNotPass() = " + op.getValue().getNotPass());
//                    System.out.println(np.getKey() + ": np.getValue().getNotPass() = " + np.getValue().getNotPass());
//                }
//                if (isNotEquals(op.getValue().getPass(), np.getValue().getPass())) {
//                    System.out.println(op.getKey() + ": op.getValue().getPass() = " + op.getValue().getPass());
//                    System.out.println(np.getKey() + ": np.getValue().getPass() = " + np.getValue().getPass());
//                }
//            });
//        }
//    }
//
//    private boolean isNotEquals(List<Integer> a, List<Integer> b) {
//        Collections.sort(a);
//        Collections.sort(b);
//        return !a.equals(b);
//    }
//
//    private boolean isNotEqualsAlt(Map<String, Map<Integer, List<Integer>>> a, Map<String, Map<Integer, List<Integer>>> b) {
//        a.forEach((k, v) -> sort(v));
//        b.forEach((k, v) -> sort(v));
//        return !a.equals(b);
//    }
//
//    private boolean isNotEquals(Map<Integer, List<Integer>> a, Map<Integer, List<Integer>> b) {
//        sort(a);
//        sort(b);
//        return !a.equals(b);
//    }
//
//    private void sort(Map<Integer, List<Integer>> map) {
//        map.forEach((k,v) -> Collections.sort(v));
//    }
//
//    private AlleleRegionStore buildStore(String chr, AlleleRegionCalculator regionCalculator, GenomeHelper helper) {
//        AlleleRegionStoreToHBaseAppendConverter toConv = new AlleleRegionStoreToHBaseAppendConverter(helper.getColumnFamily(), helper.getStudyId());
//        HBaseToAlleleRegionStoreConverter fromConv = new HBaseToAlleleRegionStoreConverter(helper, toConv.getRegionSize());
//        Collection<Append> convert = toConv.convert(chr, regionCalculator.getStore());
//        AlleleRegionStore newStore = new AlleleRegionStore(regionCalculator.getStore().getTargetRegion());
//        convert.forEach(a -> {
//            Result result = Result.create(a.getFamilyCellMap().get(helper.getColumnFamily()));
//            fromConv.convert(newStore, result);
//        });
//        return newStore;
//    }
//
//    private void process(StudyConfiguration sc, VariantTableHelper helper, Result result, Consumer<Variant> consumer) {
//        ArchiveResultToVariantConverter converter = new ArchiveResultToVariantConverter(helper.getStudyId(), helper.getColumnFamily(), sc);
//        result.listCells().forEach(cell -> {
//            List<Variant> variants = converter.convert(cell, true);
//            variants.forEach(variant -> {
//                VariantTableMapper.completeAlternateCoordinates(variant);
//                consumer.accept(variant);
//            });
//        });
//
//    }
}
