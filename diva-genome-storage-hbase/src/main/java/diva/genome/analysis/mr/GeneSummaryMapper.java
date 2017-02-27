package diva.genome.analysis.mr;

import diva.genome.analysis.models.avro.GeneSummary;
import diva.genome.storage.hbase.allele.count.converter.HBaseAlleleCountsToAllelesConverter;
import diva.genome.storage.models.alleles.avro.AlleleCount;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.alleles.avro.VariantStats;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.util.StringUtils;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.opencb.opencga.storage.core.metadata.StudyConfiguration;
import org.opencb.opencga.storage.hadoop.variant.AbstractHBaseMapReduce;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static diva.genome.storage.hbase.allele.AnalysisExportDriver.CONFIG_ANALYSIS_EXPORT_COHORTS;

/**
 * Created by mh719 on 27/02/2017.
 */
public class GeneSummaryMapper extends AbstractHBaseMapReduce<Text, IntWritable> {
    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    private volatile HBaseAlleleCountsToAllelesConverter hBaseAlleleCountsToAllelesConverter;
    private volatile byte[] studiesRow;
    private Set<String> exportCohort;
    private Map<String, Set<Integer>> cohorts;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        super.setup(context);
        StudyConfiguration sc = getStudyConfiguration();
        Set<Integer> sampleIdsInCohorts = new HashSet<>();
        cohorts = new HashMap<>();
        this.exportCohort = new HashSet<>(Arrays.asList(
                context.getConfiguration().getStrings(CONFIG_ANALYSIS_EXPORT_COHORTS, "ALL")));
        exportCohort.forEach((c) -> {
            if (sc.getCohortIds().containsKey(c)) {
                throw new IllegalStateException("Cohort does not exist: " + c);
            }
            Integer id = sc.getCohortIds().get(c);
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
    }

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException,
            InterruptedException {
        if (!isMetaRow(value)) { // ignore _METADATA row
            AllelesAvro alleles = convertToVariant(value);
            try {
                Map<String, VariantStats> stats = alleles.getStats();
                if (null == stats) {
                    throw new IllegalStateException("Stats are null");
                }
                Float opr = alleles.getOverallPassRate();
                if (opr < 0.95) {
                    context.getCounter("DIVA", "OPR-fail").increment(1);
                    return;
                }
                Float ctlMaf = getControlFrequency(stats);
                if (ctlMaf >= 0.001) {
                    context.getCounter("DIVA", "ctl-freq-high").increment(1);
                    return;
                }
                if (!hasNonsenseVariant(alleles.getConsequenceTypes())) {
                    context.getCounter("DIVA", "csq-not-nonsense").increment(1);
                    return;
                }
                if (!hasProteinCodingGene(alleles.getBioTypes())) {
                    context.getCounter("DIVA", "biotype-not-protein-coding").increment(1);
                    return;
                }
                Set<String> ensGenes = getProteinCodingNonense(alleles.getAnnotation().getConsequenceTypes());
                if (ensGenes.isEmpty()) {
                    context.getCounter("DIVA", "csq-biotype-not-found").increment(1);
                    return;
                }
                Set<Integer> affected = getAffected(alleles.getAlleleCount());
                Set<Integer> affectedCases = new HashSet<>();
                Set<Integer> affectedCtls = new HashSet<>();
                Set<Integer> ctl = cohorts.get("PAH");
                Set<Integer> cases = cohorts.get("PAH_CONTROL_UNRELATED");
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
                for (String ensGene : ensGenes) {
                    context.getCounter("DIVA", "gene-submitted").increment(1);
                    GeneSummary.Builder builder = GeneSummary.newBuilder();
                    builder.setCases(new ArrayList<>(affectedCases));
                    builder.setControls(new ArrayList<>(affectedCtls));
                    context.write(new Text(ensGene), new IntWritable(affected.size()));
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

    private Set<String> getProteinCodingNonense(List<ConsequenceType> consequenceTypes) {
        return consequenceTypes.stream().filter(csq ->
                isProteinCoding(csq.getBiotype()) &&
                        csq.getSequenceOntologyTerms().stream().anyMatch(s -> isNonsenseVariant(s.getName())))
                .map(ConsequenceType::getEnsemblGeneId).collect(Collectors.toSet());
    }

    private boolean hasProteinCodingGene(List<String> bioTypes) {
        return bioTypes.stream().anyMatch(s -> isProteinCoding(s));
    }

    private boolean isProteinCoding(String s) {
        return org.apache.commons.lang.StringUtils.equals(s, BIOTYPE_PROTEIN_CODING);
    }

    private boolean hasNonsenseVariant(List<String> consequenceTypes) {
        for (String csq : consequenceTypes) {
            if (isNonsenseVariant(csq)) return true;
        }
        return false;
    }

    private boolean isNonsenseVariant(String csq) {
        switch (csq) {
            case "transcript_ablation":
            case "splice_acceptor_variant":
            case "splice_donor_variant":
            case "stop_gained":
            case "frameshift_variant":
            case "stop_lost":
            case "start_lost":
            case "transcript_amplification":
                return true;
        }
        return false;
    }

    private Float getControlFrequency(Map<String, VariantStats> stats) {
        VariantStats controls = stats.get("PAH_CONTROL_UNRELATED");
        if (null == controls) {
            throw new IllegalStateException("Control stats are null");
        }
        return controls.getMaf();
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
