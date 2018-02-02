package vilmaa.genome.storage.hbase.allele.exporter;

import org.opencb.commons.datastore.core.QueryOptions;
import org.opencb.opencga.storage.core.variant.adaptors.VariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.adaptors.HadoopVariantSourceDBAdaptor;
import org.opencb.opencga.storage.hadoop.variant.exporters.VariantTableExportDriver;
import vilmaa.genome.analysis.models.variant.stats.VariantStatistics;
import vilmaa.genome.storage.hbase.allele.AnalysisExportDriver;
import htsjdk.variant.vcf.VCFHeaderLine;
import htsjdk.variant.vcf.VCFHeaderLineType;
import htsjdk.variant.vcf.VCFInfoHeaderLine;
import org.apache.hadoop.conf.Configuration;
import org.opencb.biodata.models.variant.StudyEntry;
import org.opencb.biodata.models.variant.Variant;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.core.variant.io.VariantVcfDataWriter;
import org.opencb.opencga.storage.hadoop.variant.exporters.HadoopVcfOutputFormat;
import org.opencb.opencga.storage.hadoop.variant.index.VariantTableHelper;

import java.io.IOException;
import java.io.OutputStream;
import java.text.DecimalFormat;
import java.util.*;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Add additional custom fields to VCF output that includes OPR, CR, PR and HWE.
 * Created by mh719 on 31/03/2017.
 */
public class HadoopVcfVilmaaOutputFormat extends HadoopVcfOutputFormat {
    private static final DecimalFormat DECIMAL_FORMAT_7 = new DecimalFormat("#.#######");
    private static final DecimalFormat DECIMAL_FORMAT_3 = new DecimalFormat("#.###");

    protected static final String OPR = "OPR";
    protected static final String CR = "CR";
    protected static final String PR = "PR";
    protected static final String HWE = "HWE";
    public static final String VILMAA_ALLELE_OUTPUT_VCF_ANNOTATION_KEY = "vilmaa.allele.output.vcf_annotation_key";
    public static final String VILMAA_ALLELE_OUTPUT_VCF_ANNOTATION = "vilmaa.allele.output.vcf_annotation";

    public HadoopVcfVilmaaOutputFormat() {
        // do nothing
    }

    @Override
    public VariantVcfDataWriter prepareVcfWriter(VariantTableHelper helper, StudyConfiguration sc,
                                                    BiConsumer<Variant, RuntimeException> failed,
                                                    OutputStream fileOut) throws IOException {
        // prepare indexes
        Configuration conf = helper.getConf();
        Set<String> validCohorts =  new HashSet<>(Arrays.asList(
                conf.getStrings(AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS, StudyEntry.DEFAULT_COHORT)));
        Map<String, Integer> cohortIds = sc.getCohortIds().entrySet().stream()
                .filter(e -> validCohorts.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        // init writer
        boolean withGenotype = helper.getConf()
                .getBoolean(VariantTableExportDriver.CONFIG_VARIANT_TABLE_EXPORT_GENOTYPE, false);
        VariantSourceDBAdaptor source = new HadoopVariantSourceDBAdaptor(helper);
        QueryOptions options = new QueryOptions();

        // add possible variant annotations
        if (!Objects.isNull(helper.getConf().get(VILMAA_ALLELE_OUTPUT_VCF_ANNOTATION, null))){
            options.put("annotations", helper.getConf().get(VILMAA_ALLELE_OUTPUT_VCF_ANNOTATION, ""));
        }

        VariantVcfDataWriter writer = new VariantVcfDataWriter(sc, source, fileOut, options);
        writer.setExportGenotype(withGenotype);
        if (null != failed) {
            writer.setConverterErrorListener(failed);
        }

        // set attributes
        writer.setCohortIds(cohortIds);
        writer.addAttributeKeyMapping(VariantVcfDataWriter.CSQ,
                helper.getConf().get(VILMAA_ALLELE_OUTPUT_VCF_ANNOTATION_KEY, "ANN"));

        /* Headers */
        List<VCFHeaderLine> customHeader = new ArrayList<>();
        validCohorts.forEach(cohort -> {
            String prefix = writer.buildCohortPrefix(cohort);
            String txt = cohort + " cohort: ";
            customHeader.add(
                    new VCFInfoHeaderLine(
                            prefix + OPR, 1,
                            VCFHeaderLineType.Float, txt + "Overall Pass Rate (CR * PR.)"));
            customHeader.add(
                    new VCFInfoHeaderLine(
                            prefix + CR, 1,
                            VCFHeaderLineType.Float, txt + "Call rate - rate of reference and variant calling in samples."));
            customHeader.add(
                    new VCFInfoHeaderLine(
                            prefix + PR, 1,
                            VCFHeaderLineType.Float, txt + "Pass rate - rate of reference and variant calling passing in samples."));
            customHeader.add(
                    new VCFInfoHeaderLine(
                            prefix + HWE, 1,
                            VCFHeaderLineType.Float, txt + "Exact two-sided Hardy-Weinberg p-value."));
            });
        writer.setCustomHeaderSupplier(() -> customHeader);

        /* OPR / PR / CR Writer */
        writer.setCustomAttributeStatsFunction((cohort, stats) -> {
            if (!(stats instanceof VariantStatistics)) {
                return Collections.emptyMap();
            }
            String prefix = writer.buildCohortPrefix(cohort);
            VariantStatistics vstats = (VariantStatistics) stats;
            Map<String, String> attr = new HashMap<>();
            nonNull(vstats.getOverallPassRate(), (opr) -> attr.put(prefix + OPR, DECIMAL_FORMAT_3.format(opr)));
            nonNull(vstats.getPassRate(), (pr) -> attr.put(prefix + PR, DECIMAL_FORMAT_3.format(pr)));
            nonNull(vstats.getCallRate(), (cr) -> attr.put(prefix + CR, DECIMAL_FORMAT_3.format(cr)));
            nonNull(vstats.getHw(), hw ->
                    nonNull(hw.getPValue(), pv -> attr.put(prefix + HWE, DECIMAL_FORMAT_7.format(pv))));
            return attr;
        });
        return writer;
    }

    public static <T> void nonNull(T value, Consumer<T> consumer) {
        if (Objects.nonNull(value)) {
            consumer.accept(value);
        }
    }
}
