package diva.genome.storage.hbase.allele.count;

import com.google.common.collect.BiMap;
import htsjdk.variant.variantcontext.Allele;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.AlternateCoordinate;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.biodata.tools.variant.merge.VariantMerger;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.converters.stats.HBaseToVariantStatsConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.*;

/**
 * Convert HBase Variant table of allele counts to Variant object including Variant annotation and statistics if set.
 * Supports {@link ResultSet} and {@link Result} provided by Phoenix query or Scan.
 *
 * Created by mh719 on 27/01/2017.
 */
public class HBaseAlleleCountsToVariantConverter {
    private Logger logger = LoggerFactory.getLogger(HBaseAlleleCountsToVariantConverter.class);

    private final List<String> returnedSamples = new ArrayList<>();
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new HashMap<>();
    private final GenomeHelper genomeHelper;
    private final StudyConfiguration studyConfiguration;
    private volatile HBaseToVariantAnnotationConverter annotationConverter;
    private volatile HBaseToVariantStatsConverter statsConverter;
    private volatile HBaseToAlleleCountConverter alleleCountConverter;

    private boolean mutableSamplesPosition = true;
    private boolean studyNameAsStudyId = false;
    private boolean parseAnnotations = false;
    private boolean parseStatistics = false;

    public HBaseAlleleCountsToVariantConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        this.genomeHelper = genomeHelper;
        this.studyConfiguration = studyConfiguration;
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
    }

    public void setAlleleCountConverter(HBaseToAlleleCountConverter alleleCountConverter) {
        this.alleleCountConverter = alleleCountConverter;
    }

    public void setMutableSamplesPosition(boolean mutableSamplesPosition) {
        this.mutableSamplesPosition = mutableSamplesPosition;
    }

    public HBaseToAlleleCountConverter getAlleleCountConverter() {
        return alleleCountConverter;
    }

    public void setParseAnnotations(boolean parseAnnotations) {
        this.parseAnnotations = parseAnnotations;
    }

    public void setParseStatistics(boolean parseStatistics) {
        this.parseStatistics = parseStatistics;
    }

    public void setStudyNameAsStudyId(boolean studyNameAsStudyId) {
        this.studyNameAsStudyId = studyNameAsStudyId;
    }

    public HBaseToVariantAnnotationConverter getAnnotationConverter() {
        if (null == annotationConverter) {
            this.annotationConverter = new HBaseToVariantAnnotationConverter(genomeHelper);
        }
        return annotationConverter;
    }

    public HBaseToVariantStatsConverter getStatsConverter() {
        if (null == statsConverter) {
            statsConverter = new HBaseToVariantStatsConverter(genomeHelper);
        }
        return statsConverter;
    }

    public VariantAnnotation parseAnnotation(Result result) {
        VariantAnnotation annot = null;
        if (parseAnnotations) {
            annot = getAnnotationConverter().convert(result);
        }
        if (annot == null) {
            annot = new VariantAnnotation();
            annot.setConsequenceTypes(Collections.emptyList());
        }
        return annot;
    }

    public VariantAnnotation parseAnnotation(ResultSet result) {
        VariantAnnotation annot = null;
        if (parseAnnotations) {
            annot = getAnnotationConverter().convert(result);
        }
        if (annot == null) {
            annot = new VariantAnnotation();
            annot.setConsequenceTypes(Collections.emptyList());
        }
        return annot;
    }

    public Map<Integer, Map<Integer, VariantStats>> parseStatistics(Result result) {
        if (parseStatistics) {
            return getStatsConverter().convert(result);
        }
        return Collections.emptyMap();
    }

    public Map<Integer, Map<Integer, VariantStats>> parseStatistics(ResultSet result) {
        if (parseStatistics) {
            return getStatsConverter().convert(result);
        }
        return Collections.emptyMap();
    }

    public void setReturnSamples(Collection<String> sampleNames) {
        this.returnedSamples.clear();
        for (String name : sampleNames) {
            this.returnedSamples.add(name);
        }
    }

    public void setReturnSampleIds(Collection<Integer> sampleIds) {
        this.returnedSamples.clear();
        BiMap<Integer, String> map = StudyConfiguration.getIndexedSamples(this.studyConfiguration).inverse();
        for (Integer sid : sampleIds) {
            String s = map.get(sid);
            this.returnedSamples.add(s);
        }
    }

    public Variant convertRowKey(byte[] variantRowKey) {
        return this.genomeHelper.extractVariantFromVariantRowKey(variantRowKey);
    }

    private void addAnnotation(Variant variant, VariantAnnotation annotation) {
        variant.setAnnotation(annotation);
        if (StringUtils.isNotEmpty(annotation.getId())) {
            variant.setId(annotation.getId());
        } else {
            variant.setId(variant.toString());
        }
    }


    private void addStatistics(Variant variant, Map<Integer, Map<Integer, VariantStats>> stats) {
        if (stats == null) {
            return;
        }

        int studyId = studyConfiguration.getStudyId();
        Map<Integer, VariantStats> convertedStatsMap = stats.get(studyId);
        if (convertedStatsMap == null) {
            return;
        }
        String studyName = studyConfiguration.getStudyName();
        StudyEntry studyEntry = variant.getStudy(studyName);
        if (null == studyEntry) {
            return;
        }

        BiMap<Integer, String> cohortIds = studyConfiguration.getCohortIds().inverse();
        Map<String, VariantStats> statsMap = new HashMap<>(convertedStatsMap.size());
        for (Map.Entry<Integer, VariantStats> entry : convertedStatsMap.entrySet()) {
            String cohortName = cohortIds.get(entry.getKey());
            statsMap.put(cohortName, entry.getValue());
        }
        studyEntry.setStats(statsMap);
    }

    private Variant convertRowKey(ResultSet resultSet) throws SQLException {
        Variant variant = new Variant(
                resultSet.getString(VariantPhoenixHelper.VariantColumn.CHROMOSOME.column()),
                resultSet.getInt(VariantPhoenixHelper.VariantColumn.POSITION.column()),
                resultSet.getString(VariantPhoenixHelper.VariantColumn.REFERENCE.column()),
                resultSet.getString(VariantPhoenixHelper.VariantColumn.ALTERNATE.column()));
        String type = resultSet.getString(VariantPhoenixHelper.VariantColumn.TYPE.column());
        if (StringUtils.isNotBlank(type)) {
            variant.setType(VariantType.valueOf(type));
        }
        return variant;
    }

    public Variant convert(ResultSet result) throws SQLException {
        Variant variant = convertRowKey(result);
        logger.debug("Fill {} with allele count ...", variant);
        AlleleCountPosition bean = convertToAlleleCount(result);
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded bean with {} ", bean.toDebugString());
        }
        variant = fillVariant(variant, bean);
        addAnnotation(variant, parseAnnotation(result));
        addStatistics(variant, parseStatistics(result));
        return variant;
    }

    public Variant convert(Result result) {
        Variant variant = convertRowKey(result.getRow());
        AlleleCountPosition bean = convertToAlleleCount(result);
        variant = fillVariant(variant, bean);
        addAnnotation(variant, parseAnnotation(result));
        addStatistics(variant, parseStatistics(result));
        return variant;
    }

    private AlleleCountPosition convertToAlleleCount(Result result) {
        return this.alleleCountConverter.convert(result);
    }

    private AlleleCountPosition convertToAlleleCount(ResultSet result) throws SQLException {
        return this.alleleCountConverter.convert(result);
    }

    public Variant fillVariant(Variant variant, AlleleCountPosition bean) {
        boolean isNoVariation = variant.getType().equals(VariantType.NO_VARIATION);
        if (StringUtils.isBlank(variant.getReference()) && variant.getType().equals(VariantType.NO_VARIATION)) {
            variant.setReference("N");
        }
        Map<String, String> attributesMap = new HashMap<>();
        Set<Integer> loadedSamples = new HashSet<>();
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        LinkedHashMap<String, Integer> returnedSamplesPosition = buildReturnSamplePositionMap();
        returnedSamplesPosition.forEach((name, position) -> loadedSamples.add(indexedSamples.get(name)));
        logger.debug("Used {} and {} map returned samples and found {} to load ...",
                this.returnedSamples.size(), returnedSamplesPosition.size(), loadedSamples.size());

        List<String> format = Arrays.asList(VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY);
        bean.filterIds(loadedSamples);
        if (loadedSamples.size() > 0) {
            calculatePassCallRates(bean, attributesMap, loadedSamples.size());
        }
        Integer nSamples = returnedSamplesPosition.size();
        List<String>[] samplesDataArray = new List[nSamples];

        BiMap<Integer, String> mapSampleIds = studyConfiguration.getSampleIds().inverse();

        /* Create Alternate Indexes */
//        bean.alternate.forEach((k, v) -> sampleWithVariant.addAll(v));  // Not needed here
        Map<String, Integer> altIndex = new HashMap<>();
        if (!isNoVariation) {
            altIndex.put(variant.getAlternate(), 1);
        }
        bean.getAltMap().forEach((k, a) -> {
            if (k.equals(DEL_SYMBOL) || k.equals(INS_SYMBOL)) {
                return;
            }
            Integer idx = altIndex.get(k);
            if (null == idx) {
                idx = altIndex.size() + 1;
                altIndex.put(k, idx);
            }
        });
        if (bean.getAltMap().containsKey(DEL_SYMBOL) && !bean.getAltMap().get(DEL_SYMBOL).isEmpty()) {
            // Variant stays reference, * as SecAlt.
            altIndex.put(DEL_SYMBOL, Math.max(2, altIndex.size() + 1));
        }
        if (!altIndex.containsKey(DEL_SYMBOL)
                && bean.getAltMap().containsKey(INS_SYMBOL) && !bean.getAltMap().get(INS_SYMBOL).isEmpty()) {
            // Variant stays reference, * as SecAlt.
            altIndex.put(DEL_SYMBOL, Math.max(2, altIndex.size() + 1));
        }

        /* Fill Sample data  */
        Set<Integer> notPassSet = new HashSet<>(bean.getNotPass());
        Map<Integer, String> sampleIdToGts = buildGts(loadedSamples, bean, altIndex, variant);
        logger.debug("Fount {} GTs ... ", sampleIdToGts.size());
        sampleIdToGts.forEach((sampleId, genotype) -> {
            String sampleName = mapSampleIds.get(sampleId);
            Integer sampleIdx = returnedSamplesPosition.get(sampleName);
            if (sampleIdx == null) {
                return;   //Sample may not be required. Ignore this sample.
            }
            String passValue = VariantMerger.PASS_VALUE;
            if (notPassSet.contains(sampleId)) {
                passValue = VariantMerger.DEFAULT_FILTER_VALUE;
            }
            List<String> lst = Arrays.asList(genotype, passValue);
            samplesDataArray[sampleIdx] = lst;
        });

        /* Alts */
        Map<Integer, String> indexAlt = new HashMap<>();
        altIndex.forEach((k, v) -> indexAlt.put(v, k));
        if (isNoVariation && indexAlt.containsKey(1)) {
            // Set Variant
            variant.setAlternate(indexAlt.get(1));
            VariantType variantType = VariantType.SNV;
            if (StringUtils.equals(variant.getAlternate(), "*")) {
                variantType = VariantType.INDEL;
            }
            variant.setType(variantType);
        }
        List<AlternateCoordinate> secAltArr = new ArrayList<>(Math.max(indexAlt.size() - 1, 0));
        for (Integer i : indexAlt.keySet().stream().filter(i -> i > 1).sorted().collect(Collectors.toList())) {
            String alternate = indexAlt.get(i);
            VariantType variantType = VariantType.SNV;
            if (StringUtils.equals(alternate, "*")) {
                variantType = VariantType.INDEL;
            }
            Integer start = variant.getStart();
            Integer end = variant.getEnd();
//            if (end < start) { // TODO not sure what to do with this.
//                // Main variant is an INDEL, but SecAlt are NOT.
//                // Needs to be the lower value, otherwise there are additional N's added
//                end = start;
//            }
            AlternateCoordinate alt = new AlternateCoordinate(
                    variant.getChromosome(), start, end,
                    variant.getReference(), alternate, variantType);
            secAltArr.add(alt);
        }

        List<List<String>> samplesData = Arrays.asList(samplesDataArray);
        StudyEntry studyEntry;
        if (studyNameAsStudyId) {
            studyEntry = new StudyEntry(studyConfiguration.getStudyName());
        } else {
            studyEntry = new StudyEntry(Integer.toString(studyConfiguration.getStudyId()));
        }
        studyEntry.setSortedSamplesPosition(returnedSamplesPosition);
        logger.debug("Add {} samples data ... ", samplesData.size());
        studyEntry.setSamplesData(samplesData);
        studyEntry.setFormat(format);
        studyEntry.setFiles(Collections.singletonList(new FileEntry("", "", attributesMap)));
        logger.debug("Add {} secAltArr data ... ", secAltArr.size());
        studyEntry.setSecondaryAlternates(secAltArr);
        logger.debug("Add study entry of {} to variant ... ", studyEntry.getStudyId());
        variant.addStudyEntry(studyEntry);
        return variant;
    }


    private LinkedHashMap<String, Integer> buildReturnSamplePositionMap() {
        LinkedHashMap<String, Integer> position = getReturnedSamplesPosition(this.studyConfiguration);
        if (mutableSamplesPosition) {
            return new LinkedHashMap<>(position);
        } else {
            return position;
        }
    }

    public Map<Integer, String> buildGts(Set<Integer> sampleIds, AlleleCountPosition bean, Map<String, Integer> altIndexs,
                                          Variant variant) {
        if (sampleIds.isEmpty()) {
            return Collections.emptyMap();
        }
        Map<Integer, String> gts = new HashMap<>();

        BiFunction<Integer, String, String> createRefGenotype = (cnt, gt) -> {
            if (StringUtils.equals(gt, Allele.NO_CALL_STRING) || StringUtils.isBlank(gt)) {
                String retGt = StringUtils.repeat("0", "/", cnt);
                if (StringUtils.isBlank(gt)) {
                    return retGt;
                } else {
                    return gt + "/" + retGt;
                }
            }
            return gt;
        };

        bean.getReference().forEach((alleles, v) -> {
            if (alleles.equals(NO_CALL)) {
                v.forEach(sid -> gts.put(sid, Allele.NO_CALL_STRING));
                return;
            }
            if (!alleles.equals(2)) { //only contains COUNTS
                String gt = createRefGenotype.apply(alleles, StringUtils.EMPTY);
                v.forEach(sid -> gts.put(sid, gt));
                return;
            }
        });

        BiConsumer<String, Map<Integer, List<Integer>>> mapGenotype = (k, a) -> {
            if (a.isEmpty()) {
                return;
            }
            if (StringUtils.equals(k, INS_SYMBOL)) {
                // Only for INSERTIONS
                if (variant.getType().equals(VariantType.INDEL) && variant.getStart() > variant.getEnd()) {
                    k = DEL_SYMBOL;
                } else {
                    // Else Ignore
                    return;
                }
            }
            Integer altIdx = altIndexs.get(k);
            if (null == altIdx) {
                throw new IllegalStateException("Problems with " + k + " for " + variant);
            }
            a.forEach((alleles, ids) -> {
                String gtTemplate = altIdx + "";
                for (int i = 1; i < alleles; i++) {
                    gtTemplate += "/" + altIdx;
                }
                String gtT = gtTemplate;
                ids.forEach(sampleId -> {
                    // for each sample ID
                    String gt = gts.get(sampleId);
                    if (StringUtils.isBlank(gt) || StringUtils.equals(gt, Allele.NO_CALL_STRING)) {
                        gt = gtT;
                    } else {
                        gt += "/" + gtT;
                    }
                    gts.put(sampleId, gt);
                });
            });
        };
        bean.getAltMap().forEach(mapGenotype);
        mapGenotype.accept(variant.getAlternate(), bean.getAlternate());
        String homRef = createRefGenotype.apply(2, null);
        sampleIds.forEach(sid -> {
            String gt = gts.get(sid);
            if (StringUtils.isBlank(gt)) {
                gts.put(sid, homRef);
            } else {
                // Order GT
                String[] split = gt.split("/");
                Arrays.sort(split);
                gt = StringUtils.join(split, '/');
                gts.put(sid, gt);
            }
        });
        for (Integer sid : new ArrayList<>(gts.keySet())) {
            if (!sampleIds.contains(sid)) {
                gts.remove(sid);
            }
        }
        return gts;
    }

    private String buildGt(Integer sample, Integer allele, Integer idx, Map<Integer, Integer> reference) {
        Integer refAllele = reference.getOrDefault(sample, 0);
        List<String> alleles = new ArrayList<>(allele + refAllele);
        for (int i = 0; i < refAllele; i++) {
            alleles.add("0");
        }
        String idxStr = idx.toString();
        for (int i = 0; i < allele; i++) {
            alleles.add(idxStr);
        }
        if (alleles.isEmpty()) {
            return Allele.NO_CALL_STRING;
        }
        return StringUtils.join(alleles, "/");
    }

    private void calculatePassCallRates(AlleleCountPosition row, Map<String, String> attributesMap, int loadedSamplesSize) {
        Integer noCall = 0;
        if (null != row.getReference().get(NO_CALL)) {
            noCall = row.getReference().get(NO_CALL).size();
        }
        Integer callCount = loadedSamplesSize - noCall;
        Integer passCount = loadedSamplesSize - row.getNotPass().size();
        attributesMap.put("PASS", passCount.toString());
        attributesMap.put("CALL", callCount.toString());
        double passRate = passCount.doubleValue() / loadedSamplesSize;
        double callRate = callCount.doubleValue() / loadedSamplesSize;
        double opr = passRate * callRate;
        attributesMap.put("PR", String.valueOf(passRate));
        attributesMap.put("CR", String.valueOf(callRate));
        attributesMap.put("OPR", String.valueOf(opr)); // OVERALL pass rate
        attributesMap.put("NS", String.valueOf(loadedSamplesSize)); // Number of Samples
    }

    /**
     * Creates a SORTED MAP with the required samples position.
     *
     * @param studyConfiguration Study Configuration
     * @return Sorted linked hash map
     */
    private LinkedHashMap<String, Integer> getReturnedSamplesPosition(StudyConfiguration studyConfiguration) {
        if (!returnedSamplesPositionMap.containsKey(studyConfiguration.getStudyId())) {
            LinkedHashMap<String, Integer> samplesPosition = StudyConfiguration.getReturnedSamplesPosition(studyConfiguration,
                    new LinkedHashSet<>(this.returnedSamples), StudyConfiguration::getIndexedSamples);
            returnedSamplesPositionMap.put(studyConfiguration.getStudyId(), samplesPosition);
        }
        return returnedSamplesPositionMap.get(studyConfiguration.getStudyId());
    }

}
