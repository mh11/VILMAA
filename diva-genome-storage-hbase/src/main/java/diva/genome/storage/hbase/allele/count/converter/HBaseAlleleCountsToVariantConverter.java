package diva.genome.storage.hbase.allele.count.converter;

import com.google.common.collect.BiMap;
import diva.genome.analysis.models.variant.stats.VariantStatistics;
import diva.genome.storage.hbase.VariantHbaseUtil;
import diva.genome.storage.hbase.allele.count.AbstractHBaseAlleleCountsConverter;
import diva.genome.storage.hbase.allele.count.AlleleCountPosition;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.count.position.HBaseAlleleCalculator.*;

/**
 * Convert HBase Variant table of allele counts to Variant object including Variant annotation and statistics if set.
 * Supports {@link ResultSet} and {@link Result} provided by Phoenix query or Scan.
 *
 * Created by mh719 on 27/01/2017.
 */
public class HBaseAlleleCountsToVariantConverter extends AbstractHBaseAlleleCountsConverter<Variant> {
    private Logger logger = LoggerFactory.getLogger(HBaseAlleleCountsToVariantConverter.class);

    public HBaseAlleleCountsToVariantConverter(GenomeHelper genomeHelper, StudyConfiguration studyConfiguration) {
        super(studyConfiguration, genomeHelper);
    }

    @Override
    protected void addAnnotation(Variant variant, VariantAnnotation annotation) {
        variant.setAnnotation(annotation);
        if (StringUtils.isNotEmpty(annotation.getId())) {
            variant.setId(annotation.getId());
        } else {
            variant.setId(variant.toString());
        }
    }

    @Override
    protected void addStatistics(Variant variant, String studyName, Map<String, VariantStatistics> statsMap) {
        StudyEntry studyEntry = variant.getStudy(studyName);
        if (null == studyEntry) {
            return;
        }
        Map<String, VariantStats> collect = statsMap.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e
                -> e.getValue()));
        studyEntry.setStats(collect);
    }

    @Override
    public Variant doConvert(Variant variant, AlleleCountPosition bean) {
        boolean isNoVariation = variant.getType().equals(VariantType.NO_VARIATION);
        if (StringUtils.isBlank(variant.getReference()) && variant.getType().equals(VariantType.NO_VARIATION)) {
            variant.setReference("N");
        }
        Map<String, String> attributesMap = new HashMap<>();
        Set<Integer> loadedSamples = new HashSet<>();
        BiMap<String, Integer> indexedSamples = StudyConfiguration.getIndexedSamples(studyConfiguration);
        LinkedHashMap<String, Integer> returnedSamplesPosition = buildReturnSamplePositionMap();
        returnedSamplesPosition.forEach((name, position) -> loadedSamples.add(indexedSamples.get(name)));
//        logger.debug("Used {} and {} map returned samples and found {} to load ...",
//                this.returnedSamples.size(), returnedSamplesPosition.size(), loadedSamples.size());

        List<String> format = Arrays.asList(VariantMerger.GT_KEY, VariantMerger.GENOTYPE_FILTER_KEY);
        bean.filterIds(loadedSamples);
        if (loadedSamples.size() > 0) {
            attributesMap.putAll(calculatePassCallRates(bean, loadedSamples.size()));
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
//        logger.debug("Fount {} GTs ... ", sampleIdToGts.size());
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
//        logger.debug("Add {} samples data ... ", samplesData.size());
        studyEntry.setSamplesData(samplesData);
        studyEntry.setFormat(format);
        studyEntry.setFiles(Collections.singletonList(new FileEntry("", "", attributesMap)));
//        logger.debug("Add {} secAltArr data ... ", secAltArr.size());
        studyEntry.setSecondaryAlternates(secAltArr);
//        logger.debug("Add study entry of {} to variant ... ", studyEntry.getStudyId());
        variant.addStudyEntry(studyEntry);
        return variant;
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
                if (VariantHbaseUtil.isInsertion(variant)) {
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

}
