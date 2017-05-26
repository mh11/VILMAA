package diva.genome.storage.hbase.allele.count;

import com.google.common.collect.BiMap;
import diva.genome.analysis.models.variant.stats.HBaseToVariantStatisticsConverter;
import diva.genome.analysis.models.variant.stats.VariantStatistics;
import diva.genome.storage.hbase.VariantHbaseUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.client.Result;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.VariantAnnotation;
import org.opencb.biodata.models.variant.avro.VariantType;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;
import org.opencb.opencga.storage.hadoop.variant.converters.annotation.HBaseToVariantAnnotationConverter;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.count.HBaseAlleleCalculator.NO_CALL;

/**
 * Created by mh719 on 24/02/2017.
 */
public abstract class AbstractHBaseAlleleCountsConverter<T> {
    private Logger logger = LoggerFactory.getLogger(AbstractHBaseAlleleCountsConverter.class);
    protected final GenomeHelper genomeHelper;
    protected final StudyConfiguration studyConfiguration;
    private final List<String> returnedSamples = new ArrayList<>();
    private final Map<Integer, LinkedHashMap<String, Integer>> returnedSamplesPositionMap = new HashMap<>();
    protected volatile HBaseToAlleleCountConverter alleleCountConverter;
    protected boolean studyNameAsStudyId = false;
    private volatile HBaseToVariantAnnotationConverter annotationConverter;
    private volatile HBaseToVariantStatisticsConverter statsConverter;
    private boolean mutableSamplesPosition = true;
    private boolean parseAnnotations = false;
    private boolean parseStatistics = false;
    private Set<String> cohortWhiteList = new HashSet<>();

    public AbstractHBaseAlleleCountsConverter(StudyConfiguration studyConfiguration, GenomeHelper genomeHelper) {
        this.studyConfiguration = studyConfiguration;
        this.alleleCountConverter = new HBaseToAlleleCountConverter();
        this.genomeHelper = genomeHelper;
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

    public HBaseToVariantStatisticsConverter getStatsConverter() {
        if (null == statsConverter) {
            Set<Integer> cohortIds = this.cohortWhiteList.stream().map(s -> studyConfiguration.getCohortIds().get(s))
                    .collect(Collectors.toSet());
            statsConverter = new HBaseToVariantStatisticsConverter(genomeHelper, genomeHelper.getColumnFamily(), genomeHelper.getStudyId(), cohortIds);
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

    public Map<Integer, Map<Integer, VariantStatistics>> parseStatistics(Result result) {
        if (parseStatistics) {
            return getStatsConverter().convert(result);
        }
        return Collections.emptyMap();
    }

    public Map<Integer, Map<Integer, VariantStatistics>> parseStatistics(ResultSet result) {
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

    public T convert(ResultSet result) throws SQLException {
        Variant variant = convertRowKey(result);
//        logger.debug("Fill {} with allele count ...", variant);
        AlleleCountPosition bean = convertToAlleleCount(result);
        if (logger.isDebugEnabled()) {
            logger.debug("Loaded bean with {} ", bean.toDebugString());
        }
        T filled = doConvert(variant, bean);
        addAnnotation(filled, parseAnnotation(result));
        addStatistics(filled, parseStatistics(result));
        return filled;
    }

    protected void addStatistics(T filled, Map<Integer, Map<Integer, VariantStatistics>> stats) {
        Map<String, VariantStatistics> statsMap = new HashMap<>();
        String studyName = studyConfiguration.getStudyName();
        if (stats != null) {
            int studyId = studyConfiguration.getStudyId();
            Map<Integer, VariantStatistics> convertedStatsMap = stats.get(studyId);
            if (convertedStatsMap != null) {
                BiMap<Integer, String> cohortIds = studyConfiguration.getCohortIds().inverse();
                for (Map.Entry<Integer, VariantStatistics> entry : convertedStatsMap.entrySet()) {
                    String cohortName = cohortIds.get(entry.getKey());
                    if (!this.cohortWhiteList.isEmpty() && this.cohortWhiteList.contains(cohortName)) {
                        statsMap.put(cohortName, entry.getValue());
                    }
                }
            }
        }
        addStatistics(filled, studyName, statsMap);
    }

    protected abstract void addStatistics(T filled, String studyName, Map<String, VariantStatistics> statsMap);

    protected abstract void addAnnotation(T filled, VariantAnnotation variantAnnotation);

    protected abstract T doConvert(Variant variant, AlleleCountPosition bean);

    public T convert(Result result) {
        Variant variant = convertRowKey(result.getRow());
        AlleleCountPosition bean = convertToAlleleCount(result);
        T filled = doConvert(variant, bean);
        addAnnotation(filled, parseAnnotation(result));
        addStatistics(filled, parseStatistics(result));
        return filled;
    }

    protected AlleleCountPosition convertToAlleleCount(Result result) {
        return this.alleleCountConverter.convert(result);
    }

    protected AlleleCountPosition convertToAlleleCount(ResultSet result) throws SQLException {
        return this.alleleCountConverter.convert(result);
    }

    protected LinkedHashMap<String, Integer> buildReturnSamplePositionMap() {
        LinkedHashMap<String, Integer> position = getReturnedSamplesPosition(this.studyConfiguration);
        if (mutableSamplesPosition) {
            return new LinkedHashMap<>(position);
        } else {
            return position;
        }
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

    public Variant convertRowKey(byte[] variantRowKey) {
        return VariantHbaseUtil.inferAndSetType(this.genomeHelper.extractVariantFromVariantRowKey(variantRowKey));
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

    public static Map<String, String> calculatePassCallRates(AlleleCountPosition row, int loadedSamplesSize) {
        Map<String, String> attributesMap = new HashMap<>();
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
        return attributesMap;
    }

    public void setCohortWhiteList(Set<String> cohortWhiteList) {
        this.cohortWhiteList = cohortWhiteList;
    }

    public Set<String> getCohortWhiteList() {
        return cohortWhiteList;
    }
}
