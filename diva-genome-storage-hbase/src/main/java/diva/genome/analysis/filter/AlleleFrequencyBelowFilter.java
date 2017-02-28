package diva.genome.analysis.filter;

import diva.genome.storage.models.alleles.avro.AllelesAvro;
import diva.genome.storage.models.alleles.avro.VariantStats;

import java.util.Map;
import java.util.function.Function;
import java.util.function.Predicate;

/**
 * Created by mh719 on 25/02/2017.
 */
public class AlleleFrequencyBelowFilter extends AbstractFilter<AllelesAvro> {
    private final String controlCohort;
    private final Float freqExclusive;

    public AlleleFrequencyBelowFilter(String controlCohort, Float freqExclusive) {
        this.controlCohort = controlCohort;
        this.freqExclusive = freqExclusive;
    }

    private boolean isRareControl(Map<String, VariantStats> stats) {
        return stats.get(controlCohort).getMaf() < freqExclusive;
    }
    @Override
    public Boolean doTest(AllelesAvro allelesAvro) {
        Map<String, VariantStats> stats = allelesAvro.getStats();
        if (null == stats || stats.isEmpty()) {
            return false;
        }
        return isRareControl(stats);
    }

}
