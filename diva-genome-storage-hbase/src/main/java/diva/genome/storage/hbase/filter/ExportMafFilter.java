package diva.genome.storage.hbase.filter;

import com.google.common.collect.BiMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.PhoenixHelper;
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Created by mh719 on 07/12/2017.
 */
public class ExportMafFilter implements IHbaseVariantFilter {
    public static final String CONFIG_ANALYSIS_EXPORT_MAF_COHORTS = "diva.genome.storage.allele.maf.cohorts";
    public static final String CONFIG_ANALYSIS_EXPORT_MAF_VALUE = "diva.genome.storage.allele.maf.cutoff";

    private Logger log = LoggerFactory.getLogger(this.getClass());

    private final byte[] columnFamily;
    protected volatile Map<String, Map<PhoenixHelper.Column, Float>> chromMafFilters = new HashMap<>();


    public ExportMafFilter(byte[] columnFamily) {
        this.columnFamily = columnFamily;
    }

    public static ExportMafFilter build(StudyConfiguration sc, Configuration conf, byte[] columnFamily) {
        ExportMafFilter filter = new ExportMafFilter(columnFamily);
        int studyId = sc.getStudyId();
        BiMap<String, Integer> cohortMap = sc.getCohortIds();

        filter.addChromosomeMafFilter(DEFAULT_CHROM, studyId, cohortMap,
                conf.getFloat(CONFIG_ANALYSIS_EXPORT_MAF_VALUE, 0.0F),
                conf.getStrings(CONFIG_ANALYSIS_EXPORT_MAF_COHORTS, new String[]{}));

        return filter;
    }

    private void addChromosomeMafFilter(String chromosome, int studyId, Map<String, Integer> cohortMap, float cutOff,
                                        String[] cohortArr) {
        if (cohortArr.length < 1) {
            return;
        }
        Function<Integer, PhoenixHelper.Column> toColumn = cid -> VariantPhoenixHelper.getMafColumn(studyId, cid);

        // sanity check
        Arrays.stream(cohortArr).filter(c -> !cohortMap.containsKey(c))
                .forEach(c -> {throw new IllegalStateException("Cohort name not known: " + c);});

        // prepare values
        Map<PhoenixHelper.Column, Float> defMap = Arrays.stream(cohortArr)
                .map(c -> toColumn.apply(cohortMap.get(c)))
                .collect(Collectors.toMap(c -> c, c -> cutOff));

        // add checks
        defMap.forEach((k,v) -> chromMafFilters.computeIfAbsent(chromosome, key -> new HashMap<>()).put(k,v));

        getLog().info("Using {} cohorts to filter on MAF {}",
                defMap.keySet().stream().map(c -> c.column()).collect(Collectors.toList()), cutOff);
    }

    @Override
    public boolean pass(Result value, Variant variant) {
        if (chromMafFilters.isEmpty()) {
            return true;
        }
        Map<PhoenixHelper.Column, Float> filterMap = this.chromMafFilters.get(DEFAULT_CHROM);
        for (Map.Entry<PhoenixHelper.Column, Float> entry : filterMap.entrySet()) {
            Float fValue = extractFloat(value, entry.getKey());
            if (null != fValue) {
                if (fValue > entry.getValue()) {
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean hasFilters() {
        return chromMafFilters.values().stream().mapToInt(v -> v.size()).sum() > 0;
    }

    private Float extractFloat(Result value, PhoenixHelper.Column field) {
        Cell cell = value.getColumnLatestCell(this.columnFamily, field.bytes());
        byte[] bytes = CellUtil.cloneValue(cell);
        if (bytes.length == 0) {
            return null;
        }
        return (Float) PFloat.INSTANCE.toObject(bytes);
    }

    public Logger getLog() {
        return log;
    }
}
