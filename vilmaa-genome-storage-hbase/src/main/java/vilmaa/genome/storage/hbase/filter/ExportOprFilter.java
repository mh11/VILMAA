package vilmaa.genome.storage.hbase.filter;

import com.google.common.collect.BiMap;
import vilmaa.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 03/07/2017.
 */
public class ExportOprFilter implements IHbaseVariantFilter {

    public static final String CONFIG_ANALYSIS_OPR_COHORTS = "vilmaa.genome.storage.allele.opr.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_VALUE = "vilmaa.genome.storage.allele.opr.cutoff";
    public static final String CONFIG_ANALYSIS_OPR_Y_COHORTS = "vilmaa.genome.storage.allele.opr.y.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_Y_VALUE = "vilmaa.genome.storage.allele.opr.y.cutoff";
    public static final String CONFIG_ANALYSIS_OPR_X_COHORTS = "vilmaa.genome.storage.allele.opr.x.cohorts";
    public static final String CONFIG_ANALYSIS_OPR_X_VALUE = "vilmaa.genome.storage.allele.opr.x.cutoff";

    private final byte[] columnFamily;
    private Logger log = LoggerFactory.getLogger(this.getClass());

    protected volatile Map<String, Map<PhoenixHelper.Column, Float>> chromOprFilters = new HashMap<>();

    public ExportOprFilter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public static ExportOprFilter build(StudyConfiguration sc, Configuration conf, byte[] columnFamily) {
        ExportOprFilter filter = new ExportOprFilter(columnFamily);
        String[] emtpyArray = {};
        filter.addDefaultOprFilter(sc,
                conf.getFloat(CONFIG_ANALYSIS_OPR_VALUE, -1f),
                conf.getStrings(CONFIG_ANALYSIS_OPR_COHORTS, emtpyArray));
        filter.addChromosomeOprFilter(CHR_Y, sc,
                conf.getFloat(CONFIG_ANALYSIS_OPR_Y_VALUE, -1f),
                conf.getStrings(CONFIG_ANALYSIS_OPR_Y_COHORTS, emtpyArray));
        filter.addChromosomeOprFilter(CHR_X, sc,
                conf.getFloat(CONFIG_ANALYSIS_OPR_X_VALUE, -1f),
                conf.getStrings(CONFIG_ANALYSIS_OPR_X_COHORTS, emtpyArray));
        return filter;
    }

    public void addDefaultOprFilter(StudyConfiguration sc, float oprCutoff, String[] cohorts) {
        addChromosomeOprFilter(DEFAULT_CHROM, sc, oprCutoff, cohorts);
    }

    public void addChromosomeOprFilter(String chromosome, StudyConfiguration sc, float oprCutoff, String[] cohorts) {
        if (null == cohorts || cohorts.length == 0) {
            return; // do nothing
        }
        int studyId = sc.getStudyId();
        BiMap<String, Integer> cohortMap = sc.getCohortIds();
        Map<PhoenixHelper.Column, Float> oprMap = buildFilterMap(
                cohorts, oprCutoff, cohortMap, cid -> AlleleTablePhoenixHelper.getOprColumn(studyId, cid));
        if (!oprMap.isEmpty()) {
            chromOprFilters.put(chromosome, oprMap);
        }
        getLog().info("Using {} cohorts to filter on MAF {}",
                chromOprFilters.getOrDefault(chromosome, Collections.emptyMap()).keySet().stream().map(c -> c.column()).collect(Collectors.toList()),
                oprCutoff);
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        return pass(value, variant.getChromosome());
    }

    @Override
    public boolean hasFilters() {
        return chromOprFilters.values().stream().mapToInt(v -> v.size()).sum() > 0;
    }

    public boolean pass(Result value, String chromosome) {
        if (this.chromOprFilters.isEmpty()) {
            return true;
        }
        Map<PhoenixHelper.Column, Float> filterMap = this.chromOprFilters.get(chromosome);
        if (null == filterMap) {
            filterMap = this.chromOprFilters.getOrDefault(DEFAULT_CHROM, Collections.emptyMap());
        }
        for (Map.Entry<PhoenixHelper.Column, Float> entry : filterMap.entrySet()) {
            Float fValue = extractFloat(value, entry.getKey());
            if (null != fValue) {
                if (fValue < entry.getValue()) {
                    return false;
                }
            }
        }
        return true;
    }


    private Float extractFloat(Result value, PhoenixHelper.Column field) {
        Cell cell = value.getColumnLatestCell(this.columnFamily, field.bytes());
        byte[] bytes = CellUtil.cloneValue(cell);
        if (bytes.length == 0) {
            return null;
        }
        return (Float) PFloat.INSTANCE.toObject(bytes);
    }

    public static Map<PhoenixHelper.Column, Float> buildFilterMap(String[] cohortArr, Float cutoff, Map<String, Integer> cohortMap, Function<Integer, PhoenixHelper.Column> toColumn) {
        if (null == cohortArr || cohortArr.length == 0) {
            return Collections.emptyMap();
        }
        Arrays.stream(cohortArr).filter(c -> !cohortMap.containsKey(c))
                .forEach(c -> {throw new IllegalStateException("Cohort name not known: " + c);});
        return Arrays.stream(cohortArr)
                .map(c -> toColumn.apply(cohortMap.get(c)))
                .collect(Collectors.toMap(c -> c, c -> cutoff));
    }

    private Logger getLog() {
        return log;
    }
}
