package diva.genome.analysis;

import diva.genome.analysis.filter.AlleleFrequencyBelowFilter;
import diva.genome.analysis.filter.OverallPassRateFilter;
import diva.genome.analysis.filter.VariantConsequence;
import diva.genome.storage.models.alleles.avro.AllelesAvro;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import org.opencb.biodata.models.variant.avro.ConsequenceType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Created by mh719 on 28/02/2017.
 */
public class GenomeAnalysis {
    public static final String BIOTYPE_PROTEIN_CODING = "protein_coding";

    private final List<Pair<String, Predicate<AllelesAvro>>> filters = new ArrayList<>();
    private final AtomicReference<Predicate<ConsequenceType>> csqFilters = new AtomicReference<>();
    private final String name;

    public GenomeAnalysis(String name) {
        this.name = name;
    }

    public void registerFilter(String name, Predicate<AllelesAvro> filter){
        filters.add(new ImmutablePair<>(name, filter));
    }

    public void setConsequenceTypeFilter(Predicate<ConsequenceType> filter) {
        if (csqFilters.get() != null) {
            throw new IllegalStateException("Consequence type filter already registered!!!");
        }
        csqFilters.set(filter);
    }

    public Set<String> getGeneIds(AllelesAvro allele, Consumer<String> failedFilter) {
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
            stream = stream.filter(c -> this.csqFilters.get().test(c));
        }
        return stream.map(ConsequenceType::getEnsemblGeneId).collect(Collectors.toSet());
    }

    public static GenomeAnalysis buildNonsenseAnalysis(String casesCohort, String controlCohort, Float ctlMaf) {
        GenomeAnalysis analysis = new GenomeAnalysis("NonsenseAnalysis");
        analysis.registerFilter("OPR", new OverallPassRateFilter(0.95F));
        analysis.registerFilter("CTL-FREQ", new AlleleFrequencyBelowFilter(controlCohort, ctlMaf));
        analysis.registerFilter("high", (a) -> a.getConsequenceTypes().stream().anyMatch(VariantConsequence::isHigh));
        analysis.registerFilter("protein_coding", (a) -> a.getBioTypes().stream().anyMatch(s -> StringUtils.equals(s, BIOTYPE_PROTEIN_CODING)));
        analysis.setConsequenceTypeFilter(c ->
                StringUtils.equals(c.getBiotype(), BIOTYPE_PROTEIN_CODING) &&
                        c.getSequenceOntologyTerms().stream().anyMatch(o -> VariantConsequence.isHigh(o.getName()))
        );
        return analysis;
    }


//    public static GenomeAnalysis buildCombinedAnalysis(String casesCohort, String controlCohort) {
//        GenomeAnalysis analysis = new GenomeAnalysis("NonsenseAnalysis");
//        analysis.registerFilter("OPR", new OverallPassRateFilter(0.95F));
//        analysis.registerFilter("CTL-FREQ", new AlleleFrequencyBelowFilter(controlCohort, 0.001F));
//        analysis.registerFilter("highOrMod", (a) -> a.getConsequenceTypes().stream().anyMatch(VariantConsequence::isHighOrModerate));
//        analysis.registerFilter("protein_coding", (a) -> a.getBioTypes().stream().anyMatch(s -> StringUtils.equals(s, BIOTYPE_PROTEIN_CODING)));
//        analysis.setConsequenceTypeFilter(c ->
//                StringUtils.equals(c.getBiotype(), BIOTYPE_PROTEIN_CODING) &&
//                        c.getSequenceOntologyTerms().stream().anyMatch(o -> VariantConsequence.isHighOrModerate(o.getName()))
//        );
//        return analysis;
//    }

}
