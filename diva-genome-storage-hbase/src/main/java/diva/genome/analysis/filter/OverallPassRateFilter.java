package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AlleleVariant;
import diva.genome.storage.models.alleles.avro.VariantStats;

import java.util.HashSet;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 28/02/2017.
 */
public class OverallPassRateFilter implements Function<AlleleVariant, Boolean>, Predicate<AlleleVariant> {

    private final Float cutoffInclusive;
    private final Set<String> cohorts;

    public OverallPassRateFilter(Float cutoffInclusive, Set<String> oprCohort) {
        this.cutoffInclusive = cutoffInclusive;
        this.cohorts = new HashSet<>(oprCohort);
    }

    @Override
    public Boolean apply(AlleleVariant AlleleVariant) {
        return test(AlleleVariant);
    }

    @Override
    public boolean test(AlleleVariant AlleleVariant) {
        for (String cohort : this.cohorts) {
            VariantStats stats = AlleleVariant.getStats().get(cohort);
            if (null != stats && null != stats.getOverallPassrate() && stats.getOverallPassrate() < this.cutoffInclusive) {
                return false;
            }
        }
        return true;
    }
}
