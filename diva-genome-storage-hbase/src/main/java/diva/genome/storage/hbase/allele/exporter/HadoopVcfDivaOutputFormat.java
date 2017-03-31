package diva.genome.storage.hbase.allele.exporter;

import diva.genome.analysis.models.variant.stats.VariantStatistics;
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
public class HadoopVcfDivaOutputFormat extends HadoopVcfOutputFormat {
    private static final DecimalFormat DECIMAL_FORMAT_7 = new DecimalFormat("#.#######");
    private static final DecimalFormat DECIMAL_FORMAT_3 = new DecimalFormat("#.###");

    protected static final String OPR = "OPR";
    protected static final String CR = "CR";
    protected static final String PR = "PR";
    protected static final String HWE = "HWE";

    public HadoopVcfDivaOutputFormat() {
        // do nothing
    }

    @Override
    protected VariantVcfDataWriter prepareVcfWriter(VariantTableHelper helper, StudyConfiguration sc,
                                                    BiConsumer<Variant, RuntimeException> failed,
                                                    OutputStream fileOut) throws IOException {
        // prepare indexes
        Configuration conf = helper.getConf();
        Set<String> validCohorts =  new HashSet<>(Arrays.asList(
                conf.getStrings(AlleleTableToVariantMapper.DIVA_EXPORT_STATS_FIELD, StudyEntry.DEFAULT_COHORT)));
        Map<String, Integer> cohortIds = sc.getCohortIds().entrySet().stream()
                .filter(e -> validCohorts.contains(e.getKey()))
                .collect(Collectors.toMap(e -> e.getKey(), e -> e.getValue()));

        // init writer
        VariantVcfDataWriter writer = super.prepareVcfWriter(helper, sc, failed, fileOut);

        // set attributes
        writer.setCohortIds(cohortIds);

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
