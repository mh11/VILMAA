package diva.genome.analysis.mr;

import com.google.common.collect.BiMap;
import diva.genome.analysis.GenomeAnalysis;
import diva.genome.analysis.models.avro.GeneSummary;
import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AlleleCount;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;
import org.opencb.opencga.storage.hadoop.variant.GenomeHelper;

import java.io.IOException;
import java.util.*;

import static diva.genome.analysis.mr.GenomeAnalysisDriver.*;
import static diva.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryMapper extends AbstractHBaseMapReduce<Text, ImmutableBytesWritable> {
    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    private volatile HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;
    private volatile GenomeAnalysis analysis;
    private volatile byte[] studiesRow;
    private Set<String> exportCohort;
    private Map<String, Set<Integer>> cohorts;
    private GeneSummaryReadWrite readWrite;

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

        String idxCohort = context.getConfiguration().get(CONFIG_ANALYSIS_ASSOC_CASES, "");
        getLog().info("Use {} as cases cohort ", idxCohort);
        String ctlCohort = context.getConfiguration().get(CONFIG_ANALYSIS_ASSOC_CTL, "");
        getLog().info("Use {} as control cohort ", ctlCohort);

        float ctlMafAuto = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_AUTO, 0.0001F);
        getLog().info("Use {} as Control MAF AUTO cutoff ", ctlMafAuto);
        float ctlMafX = context.getConfiguration().getFloat(CONFIG_ANALYSIS_FILTER_CTL_MAF_X, 0.000125F);
        getLog().info("Use {} as Control MAF X cutoff ", ctlMafX);
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
        this.analysis = GenomeAnalysis.buildAnalysis(analysistype, idxCohort, ctlCohort, ctlMafAuto, ctlMafX, opr, caddScore);
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        if (!isMetaRow(value)) { // ignore _METADATA row
            AllelesAvro alleles = convertToVariant(value);
            try {
                Set<Pair<String, String>> transcripts = this.analysis.findTranscripts(alleles, (s) -> {
                    context.getCounter("DIVA", s).increment(1);
                });

                if (transcripts.isEmpty()) {
                    return;
                }
                context.getCounter("DIVA", "passed-variant-filters").increment(1);
                Set<Integer> affected = getAffected(alleles.getAlleleCount());
                Set<Integer> affectedCases = new HashSet<>();
                Set<Integer> affectedCtls = new HashSet<>();
                Set<Integer> cases = cohorts.get("PAH");
                Set<Integer> ctl = cohorts.get("PAH_CONTROL_UNRELATED");
                affected.forEach(id -> {
                    if (ctl.contains(id)) {
                        affectedCtls.add(id);
                    }
                    if (cases.contains(id)) {
                        affectedCases.add(id);
                    }
                });
                if (cases.isEmpty() && ctl.isEmpty()) {
                    context.getCounter("DIVA", "no-cases-and-controls").increment(1);
                    return;
                }
                context.getCounter("DIVA", "variant-passed").increment(1);
                for (Pair<String, String> transcript : transcripts) {
                    context.getCounter("DIVA", "gene-submitted").increment(1);
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

    private Set<Integer> getAffected(AlleleCount count) {
        Set<Integer> affected = new HashSet<>();
        affected.addAll(count.getHet());
        affected.addAll(count.getHomVar());
        // add other possible combinations which have at lest one of this affected allele
        count.getAltAlleleCounts().forEach((k, v) -> affected.addAll(v));
        return affected;
    }

    protected boolean isMetaRow(Result value) {
        return Bytes.startsWith(value.getRow(), this.studiesRow);
    }

    protected AllelesAvro convertToVariant(Result value) {
        return this.getHBaseAlleleCountsToAllelesConverter().convert(value).build();
    }
    public HBaseAlleleCountsToAllelesConverter getHBaseAlleleCountsToAllelesConverter() {
        return hBaseAlleleCountsToAllelesConverter;
    }

}
