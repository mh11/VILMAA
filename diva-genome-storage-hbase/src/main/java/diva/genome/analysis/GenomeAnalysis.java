package diva.genome.analysis;

import diva.genome.analysis.filter.*;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.avro.ConsequenceType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiPredicate;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by mh719 on 28/02/2017.
 */
public class GenomeAnalysis {
    private static Logger LOG = LoggerFactory.getLogger(GenomeAnalysis.class);

    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    private final List<Pair<String, Predicate<AllelesAvro>>> filters = new ArrayList<>();
    private final AtomicReference<BiPredicate<AllelesAvro, ConsequenceType>> csqFilters = new AtomicReference<>();
    private final String name;

    public GenomeAnalysis(String name) {
        this.name = name;
    }

    public void registerFilter(String name, Predicate<AllelesAvro> filter){
        filters.add(new ImmutablePair<>(name, filter));
    }

    public void setConsequenceTypeFilter(BiPredicate<AllelesAvro, ConsequenceType> filter) {
        if (csqFilters.get() != null) {
            throw new IllegalStateException("Consequence type filter already registered!!!");
        }
        csqFilters.set(filter);
    }

    public Set<Pair<String, String>> findTranscripts(AllelesAvro allele, Consumer<String> failedFilter) {
        for (Pair<String, Predicate<AllelesAvro>> filter : filters) {
            if (!filter.getValue().test(allele)) {
                if (null != failedFilter) {
                    failedFilter.accept(filter.getKey());
                }
                return Collections.emptySet();
            }
        }
        Stream<ConsequenceType> stream = allele.getAnnotation().getConsequenceTypes().stream();
        if (this.csqFilters.get() != null ){
            stream = stream.filter(c -> this.csqFilters.get().test(allele, c));
        }
        return stream.map(c -> new ImmutablePair<>(c.getEnsemblGeneId(), c.getEnsemblTranscriptId())).collect(Collectors.toSet());
    }

    public static GenomeAnalysis buildAnalysis(String type, String casesCohort, String controlCohort, Float popFrequ, Float ctlMafAuto, Float ctlMafX, Float opr, Float cadd){
        LOG.info("Build {} analysis for cases {} and ctl {} with ctlMAF of {} AUTO and {} of X ...", type, casesCohort, controlCohort, ctlMafAuto, ctlMafX);
        GenomeAnalysis analysis = new GenomeAnalysis(type);
        analysis.registerFilter("OPR", new OverallPassRateFilter(opr));
        analysis.registerFilter("CTL-FREQ", new AlleleFrequencyBelowFilter(controlCohort, ctlMafAuto, ctlMafX));
        analysis.registerFilter("protein_coding", (a) -> a.getBioTypes().stream().anyMatch(s -> StringUtils.equals(s, BIOTYPE_PROTEIN_CODING)));
        analysis.registerFilter("ExAC-ALL", new PopulationAlleleFrequencyFilter(popFrequ, "EXAC", "ALL"));
        analysis.registerFilter("UK10K_TWINSUK", new PopulationAlleleFrequencyFilter(popFrequ, "UK10K_TWINSUK", "ALL"));
        analysis.registerFilter("UK10K_ALSPAC", new PopulationAlleleFrequencyFilter(popFrequ, "UK10K_ALSPAC", "ALL"));
        analysis.registerFilter("1kG_phase3", new PopulationAlleleFrequencyFilter(popFrequ, "1kG_phase3", "ALL"));
        switch (type) {
            case "nonsense":
                addNonsenseOptions(analysis);
                break;
            case "combined":
                addComnbinedOptions(analysis, cadd);
                break;
            default:
                throw new IllegalStateException("Analysis not yet supported: " + type);
        }
        LOG.info("Built {} analysis ...", analysis.name);
        return analysis;
    }

    private static BiPredicate<AllelesAvro, ConsequenceType> nonsenseConsequenceFilter = (a, c) ->
            StringUtils.equals(c.getBiotype(), BIOTYPE_PROTEIN_CODING) &&
                    c.getSequenceOntologyTerms().stream().anyMatch(o -> VariantConsequence.isHigh(o.getName()));

    private static Predicate<ConsequenceType> moderateConsequenceFilter = (c) ->
            StringUtils.equals(c.getBiotype(), BIOTYPE_PROTEIN_CODING)
                    && c.getSequenceOntologyTerms().stream().anyMatch(o -> VariantConsequence.isModerate(o.getName()));

    private static GenomeAnalysis addNonsenseOptions(GenomeAnalysis analysis) {
        analysis.registerFilter("high", (a) -> a.getConsequenceTypes().stream().anyMatch(VariantConsequence::isHigh));
        analysis.setConsequenceTypeFilter(nonsenseConsequenceFilter);
        return analysis;
    }

    private static GenomeAnalysis addComnbinedOptions(GenomeAnalysis analysis, Float cadd) {
        LOG.info("Use CADD score {} to filter ...");
        analysis.registerFilter("highOrModerate", (a) -> a.getConsequenceTypes().stream().anyMatch(VariantConsequence::isHighOrModerate));
        analysis.setConsequenceTypeFilter((a, c) ->
                    /* either nonsense (no CADD score filter) */
                    nonsenseConsequenceFilter.test(a, c)
                    /*  or CADD filter && moderate && NOT benign/tolerated */
                    || (Objects.nonNull(a.getCaddScaled()) && a.getCaddScaled() >= cadd
                            && moderateConsequenceFilter.test(c)
                            && !BenignFilter.isBenign(c.getProteinVariantAnnotation())
                    )
        );
        return analysis;
    }

}
