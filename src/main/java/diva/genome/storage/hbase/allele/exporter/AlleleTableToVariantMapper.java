package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import diva.genome.storage.hbase.allele.count.HBaseToAlleleCountConverter;
import diva.genome.storage.hbase.allele.stats.VariantTypeSummaryMapper;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.phoenix.schema.types.PFloat;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.biodata.models.variant.avro.FileEntry;
import org.opencb.biodata.models.variant.stats.VariantStats;
import org.opencb.opencga.storage.hadoop.variant.exporters.AnalysisToFileMapper;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableToVariantMapper extends AnalysisToFileMapper {

    public static final String DIVA_EXPORT_OPR_CUTOFF_INCL = "DIVA_EXPORT_OPR_CUTOFF_INCL";
    private volatile HBaseAlleleCountsToVariantConverter countsToVariantConverter;
    private VariantTypeSummaryMapper summaryMapper;
    private double oprCutoff;
    private String cohortMafField;
    private float cohortMafCutoff;
    private Set<String> validCohorts;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        countsToVariantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        this.countsToVariantConverter.setParseStatistics(true);
        this.countsToVariantConverter.setParseAnnotations(true);
        this.countsToVariantConverter.setReturnSamples(this.returnedSamples);
        summaryMapper = new VariantTypeSummaryMapper();
        this.summaryMapper.setIndexedSampleSize(getIndexedSamples().size());
        this.summaryMapper.setAlleleCountConverter(new HBaseToAlleleCountConverter());
        this.oprCutoff = context.getConfiguration().getDouble(DIVA_EXPORT_OPR_CUTOFF_INCL, 0.95);
        this.cohortMafField = context.getConfiguration().get("DIVA_EXPORT_MAF_FIELD", "2_38857_MAF");
        this.cohortMafCutoff = context.getConfiguration().getFloat("DIVA_EXPORT_MAF_CUTOFF_GT", 0.0F);
        getLog().info("Use MAF cohort {} with cutoff {} to filter ... ", this.cohortMafField, cohortMafCutoff);

        validCohorts = new HashSet<>();
        validCohorts.add("BRIDGE");
        validCohorts.add("BRIDGE_UNRELATED"); // quick hack
        getLog().info("Only export stats for {} ...", this.validCohorts);
    }

    protected boolean isMetaRow(Result value) {
        if (super.isMetaRow(value)) {
            return true;
        }
        if (oprCutoff < 0) {
            return false; // Not Meta
        }
        if (!doIncludeMaf(value)) {
            return true; // ignore if not seen in cohort.
        }
        // FALSE to keep the entry!!!
        return this.summaryMapper.calculateOpr(value) < this.oprCutoff;
    }

    protected boolean doIncludeMaf(Result value) {
        if (this.cohortMafCutoff < 0.0F) {
            return true; // not set if MAF is negative
        }
        if (StringUtils.isBlank(this.cohortMafField)) {
            return true; // not used
        }
        Cell cell = value.getColumnLatestCell(getHelper().getColumnFamily(), Bytes.toBytes(cohortMafField));
        byte[] bytes = CellUtil.cloneValue(cell);
        if (bytes.length == 0) {
            return false;
        }
        Float maf = (Float) PFloat.INSTANCE.toObject(bytes);
        if (null == maf) {
            return false; // not sure if this can happen.
        }
        return maf > cohortMafCutoff;
    }

    @Override
    protected Variant convertToVariant(Result value) {
        Variant variant = this.countsToVariantConverter.convert(value);
        StudyEntry se = variant.getStudy(getHelper().getStudyId() + "");
        Map<String, VariantStats> cleanStats = new HashMap<>();
        Map<String, VariantStats> stats = se.getStats();
        stats.forEach((k, v) -> {
            if (validCohorts.contains(k)) {
                cleanStats.put(k, v);
            }
        });
        se.setStats(cleanStats);
        FileEntry file = se.getFiles().get(0);
        file.setAttributes(new HashMap<>()); // overwrite attributes
        return variant;
    }
}
