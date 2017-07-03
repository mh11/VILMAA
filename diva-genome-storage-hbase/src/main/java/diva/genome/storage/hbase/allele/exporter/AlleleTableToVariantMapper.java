package diva.genome.storage.hbase.allele.exporter;

import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToVariantConverter;
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

import static diva.genome.storage.hbase.allele.AnalysisExportDriver.*;

/**
 * Maps HBase entries to Variant objects and allows to filter on cohort specific OPR & MAF values.
 * Created by mh719 on 05/02/2017.
 */
public class AlleleTableToVariantMapper extends AnalysisToFileMapper {

    private volatile HBaseAlleleCountsToVariantConverter countsToVariantConverter;
    private ExportOprFilter oprFilter;
    private float cohortMafCutoff;
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
        this.oprFilter = new ExportOprFilter(
                getHelper().getColumnFamily(),
                (v) -> getHelper().extractVariantFromVariantRowKey(v.getRow()).getChromosome());
        this.oprFilter.configure(getStudyConfiguration(), context.getConfiguration());

        // Cohort name to Cohort ID
        int studyId = getHelper().getStudyId();
        studyName = getStudyConfiguration().getStudyName();
        this.cohortMafField = new HashSet<>(Arrays.asList(context.getConfiguration().get(CONFIG_ANALYSIS_EXPORT_MAF_COHORTS, StudyEntry.DEFAULT_COHORT)));
        this.cohortMafCellField =  this.cohortMafField.stream().map(f ->
                VariantPhoenixHelper.getMafColumn(studyId, getStudyConfiguration().getCohortIds().get(f)).column())
                .collect(Collectors.toSet());
        this.cohortMafCutoff = context.getConfiguration().getFloat(CONFIG_ANALYSIS_EXPORT_MAF_VALUE, 0.0F);
        getLog().info("Use MAF cohort {} with {} for cutoff {} to filter ... ", this.cohortMafField, cohortMafCellField, cohortMafCutoff);

        validCohorts = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, StudyEntry.DEFAULT_COHORT)
        ));
        getLog().info("Only export stats for {} ...", this.validCohorts);
        this.countsToVariantConverter.setCohortWhiteList(validCohorts);
    }

    protected boolean isMetaRow(Result value) {
        if (super.isMetaRow(value)) {
            return true;
        }
        if (!doIncludeMaf(value)) {
            return true; // ignore if not seen in cohort.
        }
        // FALSE to keep the entry!!!
        return !doIncludeOpr(value);
    }

    protected boolean doIncludeOpr(Result value) {
        return this.oprFilter.isValidOpr(value);
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
        if (this.cohortMafCutoff < 0.0F) {
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
