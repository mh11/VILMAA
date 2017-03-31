package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.count.HBaseAlleleCountsToVariantConverter;
import diva.genome.storage.hbase.allele.transfer.AlleleTablePhoenixHelper;
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
import org.opencb.opencga.storage.hadoop.variant.index.phoenix.VariantPhoenixHelper;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Maps HBase entries to Variant objects and allows to filter on cohort specific OPR & MAF values.
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableToVariantMapper extends AnalysisToFileMapper {

    public static final String DIVA_EXPORT_OPR_CUTOFF_INCL = "DIVA_EXPORT_OPR_CUTOFF_INCL";
    protected static final String DIVA_EXPORT_STATS_FIELD = "DIVA_EXPORT_STATS_FIELD";
    protected static final String DIVA_EXPORT_OPR_FIELD = "DIVA_EXPORT_OPR_FIELD";
    protected static final String DIVA_EXPORT_MAF_CUTOFF_GT = "DIVA_EXPORT_MAF_CUTOFF_GT";
    protected static final String DIVA_EXPORT_MAF_FIELD = "DIVA_EXPORT_MAF_FIELD";
    private volatile HBaseAlleleCountsToVariantConverter countsToVariantConverter;
    private double oprCutoff;
    private float cohortMafCutoff;
    private Set<String> cohortOprFields;
    private Set<String> cohortOprCellFields;
    private Set<String> validCohorts;
    private Set<String> cohortMafField;
    private Set<String> cohortMafCellField;
    private String studyName;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        countsToVariantConverter = new HBaseAlleleCountsToVariantConverter(getHelper(), getStudyConfiguration());
        this.countsToVariantConverter.setParseStatistics(true);
        this.countsToVariantConverter.setParseAnnotations(true);
        this.countsToVariantConverter.setReturnSamples(this.returnedSamples);
        this.countsToVariantConverter.setStudyNameAsStudyId(true);
        this.oprCutoff = context.getConfiguration().getDouble(DIVA_EXPORT_OPR_CUTOFF_INCL, 0.95);
        this.cohortOprFields = new HashSet<>(Arrays.asList(
                // 100, 125 and 150 bp technical cohorts as default
                context.getConfiguration().getStrings(DIVA_EXPORT_OPR_FIELD, StudyEntry.DEFAULT_COHORT)
        ));
        // Cohort name to Cohort ID
        int studyId = getHelper().getStudyId();
        studyName = getStudyConfiguration().getStudyName();
        this.cohortOprCellFields = this.cohortOprFields.stream()
                .map(f -> AlleleTablePhoenixHelper.getOprColumn(
                        studyId,
                        getStudyConfiguration().getCohortIds().get(f)).column())
                .collect(Collectors.toSet());
        getLog().info("Use OPR cohort {} with {} for cutoff {} to filter ... ", this.cohortOprFields, this.cohortOprCellFields, oprCutoff);
        this.cohortMafField = new HashSet<>(Arrays.asList(context.getConfiguration().get(DIVA_EXPORT_MAF_FIELD, StudyEntry.DEFAULT_COHORT)));
        this.cohortMafCellField =  this.cohortMafField.stream().map(f ->
                VariantPhoenixHelper.getMafColumn(studyId, getStudyConfiguration().getCohortIds().get(f)).column())
                .collect(Collectors.toSet());
        this.cohortMafCutoff = context.getConfiguration().getFloat(DIVA_EXPORT_MAF_CUTOFF_GT, 0.0F);
        getLog().info("Use MAF cohort {} with {} for cutoff {} to filter ... ", this.cohortMafField, cohortMafCellField, cohortMafCutoff);

        validCohorts = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(DIVA_EXPORT_STATS_FIELD, StudyEntry.DEFAULT_COHORT)
        ));
        getLog().info("Only export stats for {} ...", this.validCohorts);
        this.countsToVariantConverter.setCohortWhiteList(validCohorts);
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
        return !doIncludeOpr(value);
    }

    protected boolean doIncludeOpr(Result value) {
        for (String oprField : this.cohortOprCellFields) {
            Float opr = extractFloat(value, oprField); // low level OPR access for faster processing
            if (null == opr) {
                return false;
            }
            if (opr < this.oprCutoff) {
                return false; // One OPR is below cutoff -> exclude
            }
        }
        return true;
    }

    private Float extractFloat(Result value, String field) {
        Cell cell = value.getColumnLatestCell(getHelper().getColumnFamily(), Bytes.toBytes(field));
        byte[] bytes = CellUtil.cloneValue(cell);
        if (bytes.length == 0) {
            return null;
        }
        return (Float) PFloat.INSTANCE.toObject(bytes);
    }

    protected boolean doIncludeMaf(Result value) {
        if (this.cohortMafCutoff <= 0.0F) {
            return true; // not set if MAF is negative or 0
        }
        for (String mafCellField : cohortMafCellField) {
            Float maf = extractFloat(value, mafCellField);
            if (null != maf && maf > this.cohortMafCutoff) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected Variant convertToVariant(Result value) {
        Variant variant = this.countsToVariantConverter.convert(value);
        StudyEntry se = variant.getStudy(studyName);
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
