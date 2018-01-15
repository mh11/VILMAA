package vilmaa.genome.analysis.mr;

import com.google.common.collect.BiMap;
import vilmaa.genome.analysis.GenomeAnalysis;
import vilmaa.genome.analysis.filter.OverallPassRateFilter;
import vilmaa.genome.analysis.models.avro.GeneSummary;
import vilmaa.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import vilmaa.genome.storage.models.alleles.avro.AlleleVariant;
import vilmaa.genome.storage.models.alleles.avro.Genotypes;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.opencb.biodata.models.feature.Genotype;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.*;
import java.util.function.Predicate;

import static vilmaa.genome.analysis.mr.GenomeAnalysisDriver.*;
import static vilmaa.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryMapper extends AbstractHBaseMapReduce<Text, ImmutableBytesWritable> {

    private volatile HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;
    private volatile GenomeAnalysis analysis;
    private volatile byte[] studiesRow;
    private Set<String> exportCohort;
    private Map<String, Set<Integer>> cohorts;
    private GeneSummaryReadWrite readWrite;
    private String idxCohort;
    private String ctlCohort;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        studiesRow = getHelper().generateVariantRowKey(GenomeHelper.DEFAULT_METADATA_ROW_KEY, 0);
        readWrite = new GeneSummaryReadWrite();
        StudyConfiguration sc = getStudyConfiguration();
        BiMap<String, Integer> cohortIds = sc.getCohortIds();
        getLog().info("Cohorts in sc: ", cohortIds.keySet());
        Set<Integer> sampleIdsInCohorts = new HashSet<>();
        cohorts = new HashMap<>();
        this.exportCohort = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));

        String analysistype = context.getConfiguration().get(CONFIG_ANALYSIS_MR_ANALYSISTYPE, "");
        getLog().info("Found {} analysis type ", analysistype);

        idxCohort = context.getConfiguration().get(CONFIG_ANALYSIS_ASSOC_CASES, "");
        getLog().info("Use {} as cases cohort ", idxCohort);
        ctlCohort = context.getConfiguration().get(CONFIG_ANALYSIS_ASSOC_CTL, "");
        getLog().info("Use {} as control cohort ", ctlCohort);

        float ctlMafAuto = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_AUTO, 0.0001F);
        getLog().info("Use {} as Control MAF AUTO cutoff ", ctlMafAuto);
        float ctlMafX = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_X, 0.000125F);
        getLog().info("Use {} as Control MAF X cutoff ", ctlMafX);
        float popFreq = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_POP_AF, 0.0001F);
        getLog().info("Use {} as Population AF cutoff ", popFreq);

        Set<String> oprCohorts = new HashSet<>();
        if (null != context.getConfiguration().getStrings(CONFIG_ANALYSIS_FILTER_OPR_COHORTS)){
            oprCohorts.addAll(Arrays.asList(context.getConfiguration().getStrings(CONFIG_ANALYSIS_FILTER_OPR_COHORTS)));
        }
        float opr = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_OPR, 0.95F);
        getLog().info("Use {} as OPR cutoff ", opr);

        Float caddScore = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_COMBINED_CADD, 15);

        this.exportCohort.add(idxCohort);
        this.exportCohort.add(ctlCohort);

        exportCohort.forEach((c) -> {
            getLog().info("Check Cohort {} ...", c);
            if (!cohortIds.containsKey(c)) {
                throw new IllegalStateException("Cohort does not exist: " + c);
            }
            Integer id = cohortIds.get(c);
            Set<Integer> sampleIds = sc.getCohorts().getOrDefault(id, Collections.emptySet());
            sampleIdsInCohorts.addAll(sampleIds);
            cohorts.put(c, sampleIds);
        });

        HBaseAlleleCountsToAllelesConverter converter = new
                HBaseAlleleCountsToAllelesConverter(this.getHelper(), this.getStudyConfiguration());
        converter.setReturnSampleIds(sampleIdsInCohorts);
        converter.setMutableSamplesPosition(true);
        converter.setParseAnnotations(true);
        converter.setParseStatistics(true);
        converter.setCohortWhiteList(this.exportCohort);
        hBaseAlleleCountsToAllelesConverter = converter;
        this.analysis = GenomeAnalysis.buildAnalysis(analysistype, idxCohort, ctlCohort, popFreq, ctlMafAuto, ctlMafX, opr, oprCohorts, caddScore);
        List<Predicate<AlleleVariant>> oprFilters = this.analysis.getFilters("OPR");
        if (oprFilters.size() != 1) {
            throw new IllegalStateException("Unexpected number of OPR filters found!!! " + oprFilters.size());
        }
        OverallPassRateFilter oprFilter = (OverallPassRateFilter) oprFilters.get(0);
        if (context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_OPR_X, -1F) >= 0) {
            HashSet<String> xCohort =
                    new HashSet<>(
                            Arrays.asList(
                                    context.getConfiguration().getStrings(CONFIG_ANALYSIS_FILTER_OPR_X_COHORTS)));
            oprFilter.addChromosomeFilter("X",
                    context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_OPR_X, 0.0F),
                    xCohort);
        }
        if (context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_OPR_Y, -1F) >= 0) {
            HashSet<String> xCohort =
                    new HashSet<>(
                            Arrays.asList(
                                    context.getConfiguration().getStrings(CONFIG_ANALYSIS_FILTER_OPR_Y_COHORTS)));
            oprFilter.addChromosomeFilter("Y",
                    context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_OPR_Y, 0.0F),
                    xCohort);
        }
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        if (!isMetaRow(value)) { // ignore _METADATA row
            AlleleVariant alleles = convertToVariant(value);
            try {
                Set<Pair<String, String>> transcripts = this.analysis.findTranscripts(alleles, (s) -> {
                    context.getCounter("vilmaa", s).increment(1);
                });

                if (transcripts.isEmpty()) {
                    return;
                }
                context.getCounter("vilmaa", "passed-variant-filters").increment(1);
                Set<Integer> affected = getAffected(alleles.getGenotypes());
                Set<Integer> affectedCases = new HashSet<>();
                Set<Integer> affectedCtls = new HashSet<>();
                Set<Integer> cases = cohorts.get(idxCohort);
                Set<Integer> ctl = cohorts.get(ctlCohort);
                affected.forEach(id -> {
                    if (ctl.contains(id)) {
                        affectedCtls.add(id);
                    }
                    if (cases.contains(id)) {
                        affectedCases.add(id);
                    }
                });
                if (cases.isEmpty() && ctl.isEmpty()) {
                    context.getCounter("vilmaa", "no-cases-and-controls").increment(1);
                    return;
                }
                context.getCounter("vilmaa", "variant-passed").increment(1);
                for (Pair<String, String> transcript : transcripts) {
                    context.getCounter("vilmaa", "gene-submitted").increment(1);
                    GeneSummary.Builder builder = GeneSummary.newBuilder();
                    builder.setEnsemblGeneId(transcript.getKey());
                    builder.setEnsemblTranscriptId(transcript.getValue());
                    builder.setCases(new ArrayList<>(affectedCases));
                    builder.setControls(new ArrayList<>(affectedCtls));
                    context.write(new Text(transcript.getValue()), new ImmutableBytesWritable(readWrite.write(builder.build())));
                };
            } catch (Exception e) {
                throw new IllegalStateException("Issue with variant " +
                        StringUtils.join(":", new String[]{alleles.getChromosome(), alleles.getStart().toString(), alleles.getReference(), alleles.getAlternate()}), e);
            }
        }
    }

    private Set<Integer> getAffected(Genotypes count) {
        Set<Integer> affected = new HashSet<>();
        affected.addAll(count.getHet());
        affected.addAll(count.getHomAlt());
        // add other possible combinations which have at lest one of the main ALT (index 1)
        count.getOtherGenotypes().forEach((k, v) -> { // for each GT / samplelist pair
            Genotype gt = new Genotype(k); // decode GT
            int[] allelesIdx = gt.getAllelesIdx();
            int len = allelesIdx.length;
            for (int i = 0; i < len; i++) { // iterate overGTs
                if (allelesIdx[i] == 1) { // is main ALT
                    affected.addAll(v); // ADD samples
                    return; // done
                }
            }
        });
        return affected;
    }

    protected boolean isMetaRow(Result value) {
        return Bytes.startsWith(value.getRow(), this.studiesRow);
    }

    protected AlleleVariant convertToVariant(Result value) {
        return this.getHBaseAlleleCountsToAllelesConverter().convert(value).build();
    }
    public HBaseAlleleCountsToAllelesConverter getHBaseAlleleCountsToAllelesConverter() {
        return hBaseAlleleCountsToAllelesConverter;
    }

}
