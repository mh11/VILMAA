package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AlleleVariant;
import diva.genome.storage.models.alleles.avro.VariantStats;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 28/02/2017.
 */
public class OverallPassRateFilter implements Function<AlleleVariant, Boolean>, Predicate<AlleleVariant> {

    private final Pair<Float, Set<String>> cutoffInclusive;
    private final Map<String, Pair<Float, Set<String>>> chromosomeSpecific = new HashMap<>();

    public OverallPassRateFilter(Float cutoffInclusive, Set<String> oprCohort) {
        this.cutoffInclusive = new ImmutablePair<>(cutoffInclusive, oprCohort);
    }

    public void addChromosomeFilter(String chromosome, Float cutoff, Set<String> cohorts) {
        this.chromosomeSpecific.put(chromosome, new ImmutablePair<>(cutoff, cohorts));
    }

    @Override
    public Boolean apply(AlleleVariant AlleleVariant) {
        return test(AlleleVariant);
    }

    @Override
    public boolean test(AlleleVariant AlleleVariant) {
        Pair<Float, Set<String>> filter =
                chromosomeSpecific.getOrDefault(AlleleVariant.getChromosome(), this.cutoffInclusive);
        for (String cohort : filter.getRight()) {
            VariantStats stats = AlleleVariant.getStats().get(cohort);
            if (null != stats && null != stats.getOverallPassrate() && stats.getOverallPassrate() < filter.getLeft()) {
                return false;
            }
        }
        return true;
    }
}
